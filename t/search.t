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

my @task_list = (
    { first => "John", last => "Doe",   tel => "555-456-7890" },
    { first => "John", last => "Smith", tel => "555-123-4567" },
    { first => "Jane", last => "Doe",   tel => "555-456-7890" },
);

for my $t ( @task_list ) {
    ok( $queue->add_task( $t ), "added a task" );
}

my $reserved = $queue->reserve_task;

my @found = $queue->search;

is( scalar @found, scalar @task_list, "got correct number from search()" );
my @got = map { my $h = $_; +{ map {; $_ => $h->{$_} } qw/first last tel/ } } @found;
cmp_bag( \@got,  \@task_list, "search() got all tasks" )
    or diag explain \@got;

@found = $queue->search({ last => "Smith" });
is( scalar @found, 1, "got correct number from search on last name" );
is( $found[0]{tel}, '555-123-4567', "found correct record" )
    or diag explain $found[0];

done_testing;

# COPYRIGHT
