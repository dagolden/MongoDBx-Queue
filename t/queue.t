use 5.006;
use strict;
use warnings;
use Test::More 0.96;
use Test::Deep '!blessed';

use MongoDB 0.45;
use MongoDBx::Queue;

my $conn = eval { MongoDB::Connection->new; };
plan skip_all => "No MongoDB on localhost" unless $conn;

my $db   = $conn->get_database("mongodbx_queue_test");
my $coll = $db->get_collection("queue_t");
$coll->drop;

my ( $queue, $task, $task2 );

$queue = new_ok( 'MongoDBx::Queue', [ { db => $db, name => 'queue_t' } ] );

ok( $queue->add_task( { msg => "Hello World" } ), "added a task" );

ok( $task = $queue->reserve_task, "reserved a task" );

is( $task->{msg}, "Hello World", "task has correct data" )
  or diag explain $task;

$task2 = $queue->reserve_task;

ok( !defined $task2, "another reserve finds nothing" )
  or diag explain $task2;

is( $queue->size, 1, "size() shows 1" );

is( $queue->waiting, 0, "waiting() shows 0" );

sleep 2; # let task timeout

ok( $queue->apply_timeout(1), "applied timeout to pending tasks" );

is( $queue->waiting, 1, "waiting() shows 1" );

ok( $queue->add_task( { msg => "Goodbye World" } ), "added another task" );

is( $queue->waiting, 2, "waiting() shows 2" );

ok( $task = $queue->reserve_task, "reserved a task" );

is( $queue->waiting, 1, "waiting() shows 1" );

is( $task->{msg}, "Hello World", "got first task, not second task" )
  or diag explain $task;

ok( $queue->reschedule_task($task), "rescheduled task without setting time" );

is( $queue->waiting, 2, "waiting() shows 2" );

ok( $task = $queue->reserve_task, "reserved a task" );

is( $task->{msg}, "Hello World", "got first task" )
  or diag explain $task;

ok( $queue->reschedule_task( $task, { priority => time() + 10 } ),
    "rescheduled task for later" );

ok( $task = $queue->reserve_task( { max_priority => time() + 100 } ),
    "reserved a task" );

is( $task->{msg}, "Goodbye World", "got second task" )
  or diag explain $task;

ok( $queue->remove_task($task), "removed task" );

ok( $task = $queue->reserve_task( { max_priority => time() + 100 } ),
    "reserved a task" );

ok( $queue->remove_task($task), "removed task" );

is( $queue->size, 0, "size() shows 0" );

ok( $queue->add_task( { msg => "Save for later" }, { priority => time() + 100 } ),
    "added another task scheduled for future" );

ok( !( $task = $queue->reserve_task ), "reserve_task() doesn't see future task" );

ok(
    $task = $queue->reserve_task( { max_priority => time() + 1000 } ),
    "reserve_task( {max_priority => \$future} ) retrieves future task"
);

ok( $queue->remove_task($task), "removed task" );

ok( $queue->remove_task($task), "removed task" );

is( $queue->size, 0, "size() shows 0" );

done_testing;

# COPYRIGHT
