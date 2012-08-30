use 5.006;
use strict;
use warnings;
use Test::More 0.96;

use MongoDB 0.45;
use MongoDBx::Queue;

my $conn = eval { MongoDB::Connection->new; };
plan skip_all => "No MongoDB on localhost" unless $conn;

my $db   = $conn->get_database("mongodbx_queue_test");
my $coll = $db->get_collection("queue_t");
$coll->drop;

my ( $queue, $task, $task2 );

$queue = new_ok( 'MongoDBx::Queue', [ db => $db, name => 'queue_t' ] );

ok( $queue->add_task( { msg => "Hello World" } ), "added a task" );

$task = $queue->reserve_task;

ok( $task, "reserved a task" );

is( $task->{msg}, "Hello World", "Task has correct data" )
  or diag explain $task;

$task2 = $queue->reserve_task;

ok( ! defined $task2, "Another reserve finds nothing" )
  or diag explain $task2;

done_testing;

# COPYRIGHT
