use 5.010;
use strict;
use warnings;

package MongoDBx::Queue;

# ABSTRACT: A work queue implemented with MongoDB
# VERSION

use Any::Moose;
use Const::Fast qw/const/;
use MongoDB 0.45 ();
use boolean;

const my $ID        => '_id';
const my $RESERVED  => '_r';
const my $SCHEDULED => '_s';

=method new

  my $queue = MongoDBx::Queue->new( db => $database, @options );

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

  $queue->add_task( $hashref );

Adds a task to the queue.  The hash reference will be shallow copied into the
task.  Keys must not start with underscores, which are reserved for
MongoDBx::Queue.

=cut

# XXX eventually, need to add a scheduled time to this -- xdg, 2012-08-30

sub add_task {
  my ( $self, $data ) = @_;

  $self->_coll->insert(
    {
      %$data,
      $SCHEDULED => time(),
    },
    {
      safe => $self->safe,
    }
  );
}

=method reserve_task

  $task = $queue->reserve_task;

Atomically marks and returns a task.  The task is marked in the queue as
in-progress so it can not be reserved again unless is is rescheduled or
timed-out.  The task returned is a hash reference containing the data added in
C<add_task>, including private keys for use by MongoDBx::Queue.

Tasks are returned in insertion time order with a resolution of one second.

=cut

sub reserve_task {
  my ($self) = @_;

  my $result = $self->db->run_command(
    {
      findAndModify => $self->name,
      query         => { $RESERVED => { '$exists' => boolean::false } },
      sort          => { $SCHEDULED => 1 },
      update        => { '$set' => { $RESERVED => time() } },
    },
  );

  # XXX check get_last_error? -- xdg, 2012-08-29
  if ( ref $result ) {
    return $result->{value};
  }
  else {
    die $result; # XXX docs unclear, but imply string error
  }
}

=method reschedule_task

  $queue->reschedule_task( $task );
  $queue->reschedule_task( $task, time() );
  $queue->reschedule_task( $task, $when );

Releases the reservation on a task.  If there is no second argument, the
task keeps its original priority.  If a second argument is provided, it
sets a new insertion time for the task.  The schedule is ordered by epoch
seconds, so an arbitrary past or future time can be set and affects subsequent
reservation order.

=cut

sub reschedule_task {
  my ($self, $task, $epochsecs) = @_;
  $epochsecs //= $task->{$SCHEDULED}; # default to original time
  $self->_coll->update(
    { $ID => $task->{$ID} },
    { '$unset'  => { $RESERVED => 0 }, '$set' => { $SCHEDULED => $epochsecs } },
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

  my $queue = MongoDBx::Queue->new( db => $database );

  $queue->add_task( { msg => "Hello World" } );
  $queue->add_task( { msg => "Goodbye World" } );

  while ( my $task = $queue->reserve_task ) {
    say $task->{msg};
    $queue->remove_task( $task );
  }

=head1 DESCRIPTION

B<ALPHA> -- this is an early release and is still in development.  Testing
and feedback welcome.

MongoDBx::Queue implements a simple message queue using MongoDB as a backend.

On a single host with MongoDB, it provides a zero-configuration message service
across local applications.  Alternatively, it can use a MongoDB database
cluster that provides replication and fail-over for an even more durable queue.

Features:

=for :list
* hash references, not objects
* arbitrary message fields
* stalled tasks can be timed-out
* task rescheduling

Not yet implemented:

=for :list
* arbitrary scheduling on insertion
* parameter checking
* error handling

Warning: do not use with capped collections, as the queued messages will not
meet the constraints required by a capped collection.

=cut

# vim: ts=2 sts=2 sw=2 et:
