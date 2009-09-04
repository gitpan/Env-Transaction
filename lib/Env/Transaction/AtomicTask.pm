package Env::Transaction::AtomicTask;

use base qw(Env::Transaction::Task);

sub new
{
	my $task = __PACKAGE__->SUPER::new(@_[1..$#_]);
	if (defined $task) {
		bless $task, __PACKAGE__;
	}
	return $task;
}
1;
