use 5.010;
use strict;
use warnings;

package MongoDBx::Queue;

# ABSTRACT: A message queue implemented with MongoDB
# VERSION

use Any::Moose;
use MongoDB 0.45 ();
use boolean;

my $ID       => '_id';
my $RESERVED => '_r';
my $PRIORITY => '_p';

=method new

  $queue = MongoDBx::Queue->new( { db => $database, %options } );

Creates and returns a new queue object.  The C<db> argument is required.
Other attributes may be provided as well.

=attr db

A MongoDB::Database object to hold the queue.  Required.

=cut

has db => (
    is       => 'ro',
    isa      => 'MongoDB::Database',
    required => 1,
);

=attr name

A collection name for the queue.  Defaults to 'queue'.  The collection must
only be used by MongoDBx::Queue or unpredictable awful things will happen.

=cut

has name => (
    is      => 'ro',
    isa     => 'Str',
    default => 'queue',
);

=attr safe

Boolean that controls whether 'safe' inserts/updates are done.
Defaults to true.

=cut

has safe => (
    is      => 'ro',
    isa     => 'Bool',
    default => 1,
);

# Internal collection attribute

has _coll => (
    is         => 'ro',
    isa        => 'MongoDB::Collection',
    lazy_build => 1,
);

sub _build__coll {
    my ($self) = @_;
    return $self->db->get_collection( $self->name );
}

# Methods

=method add_task

  $queue->add_task( \%message, \%options );

Adds a task to the queue.  The C<\%message> hash reference will be shallow
copied into the task and not include objects except as described by
L<MongoDB::DataTypes>.  Top-level keys must not start with underscores, which are
reserved for MongoDBx::Queue.

The C<\%options> hash reference is optional and may contain the following key:

=for :list
* C<priority>: sets the priority for the task. Defaults to C<time()>.

Note that setting a "future" priority may cause a task to be invisible
to C<reserve_task>.  See that method for more details.

=cut

sub add_task {
    my ( $self, $data, $opts ) = @_;

    $self->_coll->insert( { %$data, $PRIORITY => $opts->{priority} // time(), },
        { safe => $self->safe, } );
}

=method reserve_task

  $task = $queue->reserve_task;
  $task = $queue->reserve_task( \%options );

Atomically marks and returns a task.  The task is marked in the queue as
"reserved" (in-progress) so it can not be reserved again unless is is
rescheduled or timed-out.  The task returned is a hash reference containing the
data added in C<add_task>, including private keys for use by MongoDBx::Queue
methods.

Tasks are returned in priority order from lowest to highest.  If multiple tasks
have identical, lowest priorities, their ordering is undefined.  If no tasks
are available or visible, it will return C<undef>.

The C<\%options> hash reference is optional and may contain the following key:

=for :list
* C<max_priority>: sets the maximum priority for the task. Defaults to C<time()>.

The C<max_priority> option controls whether "future" tasks are visible.  If
the lowest task priority is greater than the C<max_priority>, this method
returns C<undef>.

=cut

sub reserve_task {
    my ( $self, $opts ) = @_;

    my $now    = time();
    my $result = $self->db->run_command(
        {
            findAndModify => $self->name,
            query         => {
                $PRIORITY => { '$lte' => $opts->{max_priority} // $now },
                $RESERVED => { '$exists' => boolean::false },
            },
            sort => { $PRIORITY => 1 },
            update => { '$set' => { $RESERVED => $now } },
        },
    );

    # XXX check get_last_error? -- xdg, 2012-08-29
    if ( ref $result ) {
        return $result->{value}; # could be undef if not found
    }
    else {
        die "MongoDB error: $result"; # XXX docs unclear, but imply string error
    }
}

=method reschedule_task

  $queue->reschedule_task( $task );
  $queue->reschedule_task( $task, \%options );

Releases the reservation on a task so it can be reserved again.

The C<\%options> hash reference is optional and may contain the following key:

=for :list
* C<priority>: sets the priority for the task. Defaults to the task's original priority.

Note that setting a "future" priority may cause a task to be invisible
to C<reserve_task>.  See that method for more details.

=cut

sub reschedule_task {
    my ( $self, $task, $opts ) = @_;
    $self->_coll->update(
        { $ID => $task->{$ID} },
        {
            '$unset' => { $RESERVED => 0 },
            '$set'   => { $PRIORITY => $opts->{priority} // $task->{$PRIORITY} },
        },
        { safe => $self->safe }
    );
}

=method remove_task

  $queue->remove_task( $task );

Removes a task from the queue (i.e. indicating the task has been processed).

=cut

sub remove_task {
    my ( $self, $task ) = @_;
    $self->_coll->remove( { $ID => $task->{$ID} } );
}

=method apply_timeout

  $queue->apply_timeout( $seconds );

Removes reservations that occurred more than C<$seconds> ago.  If no
argument is given, the timeout defaults to 120 seconds.  The timeout
should be set longer than the expected task processing time, so that
only dead/hung tasks are returned to the active queue.

=cut

sub apply_timeout {
    my ( $self, $timeout ) = @_;
    $timeout //= 120;
    my $cutoff = time() - $timeout;
    $self->_coll->update(
        { $RESERVED => { '$lt'     => $cutoff } },
        { '$unset'  => { $RESERVED => 0 } },
        { safe => $self->safe, multiple => 1 }
    );
}

=method search

  my @results = $queue->search( \%query, \%options );

Returns a list of tasks in the queue based on search criteria.  The
query should be expressed in the usual MongoDB fashion.  In addition
to MongoDB options C<limit>, C<skip> and C<sort>, this method supports
a C<reserved> option.  If present, results will be limited to reserved
tasks if true or unreserved tasks if false.

=cut

sub search {
    my ( $self, $query, $opts ) = @_;
    $query = {} unless ref $query eq 'HASH';
    $opts  = {} unless ref $opts eq 'HASH';
    if ( exists $opts->{reserved} ) {
        $query->{$RESERVED} =
          { '$exists' => $opts->{reserved} ? boolean::true : boolean::false };
        delete $opts->{reserved};
    }
    my $cursor = $self->_coll->query( $query, $opts );
    if ( $opts->{fields} && ref $opts->{fields} ) {
        my $spec =
          ref $opts->{fields} eq 'HASH'
          ? $opts->{fields}
          : { map { $_ => 1 } @{ $opts->{fields} } };
        $cursor->fields($spec);
    }
    return $cursor->all;
}

=method peek

  $task = $queue->peek( $task );

Retrieves a full copy of the task from the queue.  This is useful to retrieve all
fields from a partial-field result from C<search>.  It is equivalent to:

  $self->search( { _id => $task->{_id} } );

Returns undef if the task is not found.

=cut

sub peek {
    my ( $self, $task ) = @_;
    my @result = $self->search( { $ID => $task->{$ID} } );
    return wantarray ? @result : $result[0];
}

=method size

  $queue->size;

Returns the number of tasks in the queue, including in-progress ones.

=cut

sub size {
    my ($self) = @_;
    return $self->_coll->count;
}

=method waiting

  $queue->waiting;

Returns the number of tasks in the queue that have not been reserved.

=cut

sub waiting {
    my ($self) = @_;
    return $self->_coll->count( { $RESERVED => { '$exists' => boolean::false } } );
}

no Any::Moose;
__PACKAGE__->meta->make_immutable;

1;

=head1 SYNOPSIS

  use v5.10;
  use MongoDB;
  use MongoDBx::Queue;

  my $connection = MongoDB::Connection->new( @parameters );
  my $database = $connection->get_database("queue_db");

  my $queue = MongoDBx::Queue->new( { db => $database } );

  $queue->add_task( { msg => "Hello World" } );
  $queue->add_task( { msg => "Goodbye World" } );

  while ( my $task = $queue->reserve_task ) {
    say $task->{msg};
    $queue->remove_task( $task );
  }

=head1 DESCRIPTION

B<ALPHA> -- this is an early release and is still in development.  Testing
and feedback welcome.

MongoDBx::Queue implements a simple, prioritized message queue using MongoDB as
a backend.  By default, messages are prioritized by insertion time, creating a
FIFO queue.

On a single host with MongoDB, it provides a zero-configuration message service
across local applications.  Alternatively, it can use a MongoDB database
cluster that provides replication and fail-over for an even more durable,
multi-host message queue.

Features:

=for :list
* messages as hash references, not objects
* arbitrary message fields
* arbitrary scheduling on insertion
* atomic message reservation
* stalled reservations can be timed-out
* task rescheduling

Not yet implemented:

=for :list
* parameter checking
* error handling

Warning: do not use with capped collections, as the queued messages will not
meet the constraints required by a capped collection.

=cut

# vim: ts=2 sts=2 sw=2 et:
