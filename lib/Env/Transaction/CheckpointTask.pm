package Env::Transaction::CheckpointTask;

use base qw(Env::Transaction::Task);

sub CPT_DUMMY_RUN { 1; }

sub new
{
	shift;
	if (@_ & 1) { croak("bad args"); }
	my %args = @_;
	if (defined $args{'run'}) {
		carp('checkpoint tasks can\'t have a \'run\' method');
		return wantarray? () : undef;
	} else {
		if (defined $args{'rc'}) {
			carp('checkpoint tasks can\'t have a \'rc\' method');
			return wantarray? () : undef;
		}
	}
	$args{'run'} = [ \&CTP_DUMMY_RUN ];
	$args{'rc'} = [ \&CTP_DUMMY_RUN ];
	my $task = __PACKAGE__->SUPER::new(%args);
	if (defined $task) {
		bless $task, __PACKAGE__;
	}
	return $task;
}

1;
