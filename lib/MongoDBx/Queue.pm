use 5.010;
use strict;
use warnings;

package MongoDBx::Queue;

# ABSTRACT: A work queue implemented with MongoDB
# VERSION

use Any::Moose;
use Const::Fast qw/const/;
use boolean;

const my $RESERVED  => '_r';
const my $SCHEDULED => '_s';

has db => (
  is       => 'ro',
  isa      => 'MongoDB::Database',
  required => 1,
);

has name => (
  is      => 'ro',
  isa     => 'Str',
  default => 'queue',
);

has safe => (
  is      => 'ro',
  isa     => 'Bool',
  default => 1,
);

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

sub reserve_task {
  my ($self) = @_;

  my $result = $self->db->run_command(
    {
      findAndModify => $self->name,
      query         => { $RESERVED => { '$exists' => boolean::false } },
      update        => { '$set' => { $RESERVED => time() } },
    },
  );
  # XXX check get_last_error? -- xdg, 2012-08-29
  if ( ref $result ) {
    return $result->{value};
  }
  else {
    die $result;
  }
}

no Any::Moose;
__PACKAGE__->meta->make_immutable;

1;

# vim: ts=2 sts=2 sw=2 et:
