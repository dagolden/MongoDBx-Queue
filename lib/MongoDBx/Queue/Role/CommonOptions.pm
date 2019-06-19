use 5.010;
use strict;
use warnings;

package MongoDBx::Queue::Role::CommonOptions;

our $VERSION = '2.001';

use Moose::Role 2;
use MooseX::Types::Moose qw/:all/;
use MooseX::AttributeShortcuts;

has database_name => (
    is      => 'ro',
    isa     => Str,
    default => 'test',
);

has client_options => (
    is      => 'ro',
    isa     => HashRef,
    default => sub { {} },
);

has collection_name => (
    is      => 'ro',
    isa     => Str,
    default => 'queue',
);

1;
