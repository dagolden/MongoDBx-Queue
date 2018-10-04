use 5.010;
use strict;
use warnings;

package MongoDBx::Queue;

# ABSTRACT: A message queue implemented with MongoDB

our $VERSION = '2.001';

use Moose 2;
use MooseX::Types::Moose qw/:all/;
use MooseX::AttributeShortcuts;

use MongoDB 2 ();
use Tie::IxHash;
use boolean;
use namespace::autoclean;

my $ID       = '_id';
my $RESERVED = '_r';
my $PRIORITY = '_p';

with 'MooseX::Role::Logger', 'MooseX::Role::MongoDB' => { -version => 1.000 };

#--------------------------------------------------------------------------#
# Public attributes
#--------------------------------------------------------------------------#

=attr database_name

A MongoDB database name.  Unless a C<db_name> is provided in the
C<client_options> attribute, this database will be the default for
authentication.  Defaults to 'test'

=cut

has database_name => (
    is      => 'ro',
    isa     => Str,
    default => 'test',
);

=attr client_options

A hash reference of L<MongoDB::MongoClient> options that will be passed to its
C<connect> method.

=cut

has client_options => (
    is      => 'ro',
    isa     => HashRef,
    default => sub { {} },
);

=attr collection_name

A collection name for the queue.  Defaults to 'queue'.  The collection must
only be used by MongoDBx::Queue or unpredictable awful things will happen.

=cut

has collection_name => (
    is      => 'ro',
    isa     => Str,
    default => 'queue',
);

sub _build__mongo_default_database { $_[0]->database_name }

sub _build__mongo_client_options {
    return {
        write_concern => { w     => "majority" },
        read_concern  => { level => "majority" },
        %{ $_[0]->client_options },
    };
}

sub BUILD {
    my ($self) = @_;
    # ensure index on PRIORITY in the same order we use for reserving
    $self->_mongo_collection( $self->collection_name )
      ->indexes->create_one( [ $PRIORITY => 1 ] );
}

#--------------------------------------------------------------------------#
# Public methods
#--------------------------------------------------------------------------#

=method new

   $queue = MongoDBx::Queue->new(
        database_name   => "my_app",
        client_options  => {
            host => "mongodb://example.net:27017",
            username => "willywonka",
            password => "ilovechocolate",
        },
   );

Creates and returns a new queue object.

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

    $self->_mongo_collection( $self->collection_name )
      ->insert_one( { %$data, $PRIORITY => $opts->{priority} // time() } );
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

    my $now = time();
    return $self->_mongo_collection( $self->collection_name )->find_one_and_update(
        {
            $PRIORITY => { '$lte'    => $opts->{max_priority} // $now },
            $RESERVED => { '$exists' => boolean::false },
        },
        { '$set' => { $RESERVED => $now } },
        { sort   => [ $PRIORITY => 1 ] },
    );
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
    $self->_mongo_collection( $self->collection_name )->update_one(
        { $ID => $task->{$ID} },
        {
            '$unset' => { $RESERVED => 0 },
            '$set'   => { $PRIORITY => $opts->{priority} // $task->{$PRIORITY} },
        },
    );
}

=method remove_task

  $queue->remove_task( $task );

Removes a task from the queue (i.e. indicating the task has been processed).

=cut

sub remove_task {
    my ( $self, $task ) = @_;
    $self->_mongo_collection( $self->collection_name )
      ->delete_one( { $ID => $task->{$ID} } );
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
    $self->_mongo_collection( $self->collection_name )->update_many(
        { $RESERVED => { '$lt'     => $cutoff } },
        { '$unset'  => { $RESERVED => 0 } },
    );
}

=method search

  my @results = $queue->search( \%query, \%options );

Returns a list of tasks in the queue based on search criteria.  The
query should be expressed in the usual MongoDB fashion.  In addition
to MongoDB options (e.g. C<limit>, C<skip> and C<sort>) as described
in the MongoDB documentation for L<MongoDB::Collection/find>, this method
supports a C<reserved> option.  If present, results will be limited to reserved
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
    my $cursor =
      $self->_mongo_collection( $self->collection_name )->find( $query, $opts );
    return $cursor->all;
}

=method peek

  $task = $queue->peek( $task );

Retrieves a full copy of the task from the queue.  This is useful to retrieve all
fields from a projected result from C<search>.  It is equivalent to:

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
    return $self->_mongo_collection( $self->collection_name )->count_documents( {} );
}

=method waiting

  $queue->waiting;

Returns the number of tasks in the queue that have not been reserved.

=cut

sub waiting {
    my ($self) = @_;
    return $self->_mongo_collection( $self->collection_name )
      ->count_documents( { $RESERVED => { '$exists' => boolean::false } } );
}

__PACKAGE__->meta->make_immutable;

1;

=for Pod::Coverage BUILD

=head1 SYNOPSIS

    use v5.10;
    use MongoDBx::Queue;

    my $queue = MongoDBx::Queue->new(
        database_name => "queue_db",
        client_options => {
            host => "mongodb://example.net:27017",
            username => "willywonka",
            password => "ilovechocolate",
        }
    );

    $queue->add_task( { msg => "Hello World" } );
    $queue->add_task( { msg => "Goodbye World" } );

    while ( my $task = $queue->reserve_task ) {
        say $task->{msg};
        $queue->remove_task( $task );
    }

=head1 DESCRIPTION

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
* automatically creates correct index
* fork-safe

Not yet implemented:

=for :list
* parameter checking
* error handling

Warning: do not use with capped collections, as the queued messages will not
meet the constraints required by a capped collection.

=cut

# vim: ts=4 sts=4 sw=4 et:
