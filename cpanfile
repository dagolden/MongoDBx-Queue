requires "MongoDB" => "0.702";
requires "Moose" => "2";
requires "MooseX::AttributeShortcuts" => "0";
requires "MooseX::Role::MongoDB" => "0";
requires "MooseX::Types::Moose" => "0";
requires "Tie::IxHash" => "0";
requires "boolean" => "0";
requires "namespace::autoclean" => "0";
requires "perl" => "5.010";
requires "strict" => "0";
requires "warnings" => "0";

on 'test' => sub {
  requires "ExtUtils::MakeMaker" => "0";
  requires "File::Spec::Functions" => "0";
  requires "File::Temp" => "0";
  requires "IO::Handle" => "0";
  requires "IPC::Open3" => "0";
  requires "List::Util" => "0";
  requires "Test::Deep" => "0";
  requires "Test::More" => "0.96";
};

on 'configure' => sub {
  requires "ExtUtils::MakeMaker" => "6.30";
};

on 'develop' => sub {
  requires "Pod::Coverage::TrustPod" => "0";
  requires "Test::CPAN::Meta" => "0";
  requires "Test::Pod" => "1.41";
  requires "Test::Pod::Coverage" => "1.08";
};
