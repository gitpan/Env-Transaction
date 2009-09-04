package Env::Transaction::Reaper;
use warnings;
use strict;
use UNIVERSAL qw(isa can);
require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(tx_reapsub);

#sub tx_mksig 
#{
#	my ($pkg) = ( $_[3] ) ? ( $_[3] =~ /^(.*)::[^:]+$/x ) : $_[0];
#	$pkg = 'main' unless $pkg;
#	return $pkg;
#};

my $loadmod = sub () {
	my $victim = shift;
	my $victim_name = join( '/', ( split /::/, $victim ) ) . '.pm';
	if ( exists $INC{$victim_name} ) {
		return 1 if $INC{$victim_name};
		carp("can't make hostage from: $victim. victim fights back	");
		return wantarray?():undef;
	}
	my ( $victim_home, $result );
  LOOPIT:
	{
		foreach my $libdir ( @_, @INC ) {
			$victim_home = "$libdir/$victim_name";
			if ( -f $victim_home ) {
				$INC{$victim_name} = $victim_home;
				$result = do $victim_home;
				last LOOPIT;
			}
		}
		carp("can't find victim");
		return wantarray?():undef;
	}
	if ($@) {
		$INC{$victim_name} = undef;
		carp(@_);
		return wantarray?():undef;
	}
	unless ($result) {
		delete $INC{$victim_name};
		carp("$victim_home did not return a true value");
		return wantarray?():undef;
	}
	return 1;
};

sub tx_reapsub 
{
	my %args     = @_;
	my $victim   = delete $args{target};
	my $fetus    = delete $args{method};
	my $selfcall = 1;
	$selfcall = delete $args{selfcall} if defined $args{selfcall};
	my $loadfrom = delete $args{fromfile};
	return wantarray?():undef unless $victim and $fetus;
	unless ( ref $victim ) {
		my $result = &{$loadmod}(
			$victim,
			UNIVERSAL::isa( $loadfrom, 'ARRAY' ) ? @{$loadfrom} : $loadfrom
		);
		unless ($result) {
			carp("can't make hostage: $victim");
			return wantarray?():undef;
		}
	}
	my $sub = UNIVERSAL::can( $victim, $fetus );
	unless ($sub) {
		carp("$victim can't do $fetus");
		return wantarray?():undef;
	}
	if ($selfcall) {
		return sub { return &{$sub}( $victim, @_ ); }
	}
	return sub { return &{$sub}(@_); }

};

1;

__END__


=head1 USAGE

use Env::Transaction::Reaper

$sub = tx_reapsub(target=>'Some::Pkg',method=>'method_name');

$sub = tx_reapsub(target=>$objref,method=>'objmeth');

=head1 TODO

tx_reapsyscmd

=head1 AUTHOR

adrian ilarion ciobanu, E<lt>cia@mud.roE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2009 by adrian ilarion ciobanu

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut
