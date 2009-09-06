package Env::Transaction;
use Data::Dumper;

use 5.008008;
use strict;
use warnings;
use Carp;
require Exporter;
use Env::Transaction::Task;
use Env::Transaction::AtomicTask;
use Env::Transaction::CheckpointTask;
sub DEBUG { return 0; }
our $VERSION = '0.00_02';
## no critic
$VERSION = eval $VERSION;  # see L<perlmodstyle>
## use critic
#require Exporter;
#our @ISA = qw(Exporter);
use base qw(Exporter);
# dont drive the developer mad by explictly asking him for names with EXPORT_OK
# nor i want to do export tags right now.
## no critic
our @EXPORT = qw(TX_ONE_PHASE_COMMIT TX_TWO_PHASE_COMMIT);
## use critic

sub TX { return 0; }
sub TASK { return 1; }
sub TX_ONE_PHASE_COMMIT { return 0x1; }
sub TX_TWO_PHASE_COMMIT { return 0x2; }
sub TX_DUMMY { return 1; }
sub ST_TX_FAILED { return 0xff; }
sub ST_TX_INIT { return 0x02 ; }
sub ST_TX_PREPARE { return 0x03; }
sub ST_TX_COMMIT { return 0x04; }
sub ST_TX_OK { return 0x01; }
sub ST_TX_FINI { return 0xaa; }
sub tx_failed { return $_[TX]->{state} == ST_TX_FAILED; }
sub tx_mark_failed { return $_[TX]->{state} = ST_TX_FAILED; }
## no critic
sub __tx_debug { print STDERR $_[2].": txid=".$_[TX]->{id}.", task_id=".__TX_task_id($_[TX],$_[TASK])."\n"; }
## use critic


sub id { return $_[TX]->{id}; }
sub new
{
	shift @_;
	if (@_ & 1) { croak "bad args"; }
	my %args = @_;
	my $tx = {
		taskq => [],
		rstack => [],
		taskmap => {},
		state => ST_TX_INIT,
	};
	$tx->{id} = sprintf "%lu.%lu", $$, $tx+0;
	if (defined $args{protocol}) {
		if ($args{protocol} == TX_ONE_PHASE_COMMIT || $args{protocol} == TX_TWO_PHASE_COMMIT) {
			$tx->{protocol} = delete $args{protocol};
		} else {
			carp('unknown commit protocol');
			return wantarray? () : undef;
		}
	} else {
		carp('setting default protocol to TX_ONE_PHASE_COMMIT');
		$tx->{protocol} = TX_ONE_PHASE_COMMIT;
	} 
	if (defined $args{autorollback}) {
		$tx->{autorollback} = 1;
	}
	return bless $tx, __PACKAGE__;
}

sub __rb
{
	$_[TX]->{autorollback} and $_[TX]->rollback();
}
sub prepare
{
	if ($_[TX]->tx_failed()) {
		carp('transaction was marked as failed, won\'t continue');
		return wantarray? () : undef;
	}
	foreach my $task (@_[1..$#_]) {
		DEBUG and print STDERR "prepare task ". $task->name()."\n";
		if ($task->failed()) {
			carp ("task marked failed , wont prepare");
			$_[TX]->__rb();
			return wantarray? () : undef;
		}
		my $task_id = __TX_task_id($_[TX],$task);
		if ( $task_id ) {
			carp(   "duplicate task! task". $task->name()." already registered  for you (id="
				  . $task_id
				  . ")" );
			$task->mark_failed();
			#if ( $_[TX]->{protocol} == TX_TWO_PHASE_COMMIT ) {
			#	$_[TX]->rollback();
			#}
			$_[TX]->__rb();
			$_[TX]->tx_mark_failed();
			return wantarray ? () : undef;
		}
		#$task->mark_ok();
		push @{ $_[TX]->{taskq} }, $task;
		__TX_task_id($_[TX],$task,$#{  $_[TX]->{taskq}  });
		if ( $_[TX]->{protocol} == TX_TWO_PHASE_COMMIT ) {
			DEBUG and print "two phase commit mode, doing immediate commit for task ". $task->name()."\n";
			if ( !$_[TX]->commit() ) {
				#$_[TX]->rollback();
				$_[TX]->tx_mark_failed();
				return wantarray? () : undef;
			} else {
				DEBUG and __tx_debug($_[TX], $task, 'task_commit_ok');
			}
		} else {
			DEBUG and __tx_debug($_[TX],$task,'task_prepare_ok');
		}
	}
	return 1;
}


sub commit
{
	if ($_[TX]->tx_failed()) {
		carp('transaction marker as failed, wont commit again');
		return wantarray?():undef;
	}
	if (  $_[TX]->{protocol} == TX_TWO_PHASE_COMMIT ) {
		if ( $#{  $_[TX]->{taskq}  } == -1 ) {
			DEBUG and print "call commit() on TX with no task in two-phase commit mode, assuming callbacks\n";
			$_[TX]->__TX_run_cb();
			$_[TX]->{state} = ST_TX_FINI;
			return 1;
		}
		if ( $#{  $_[TX]->{taskq}  } > 0 ) { #we should have only one task in queue
			carp('COMMIT: found uncommited tasks in queue while in TX_TWO_PHASE_COMMIT mode!'.$#{ @{ $_[TX]->{taskq} } });
			$_[TX]->__rb();
			$_[TX]->tx_mark_failed();
			return wantarray?():undef;
		}
	}
	#run the tasks
	while (my $task = shift @{ $_[TX]->{taskq} }) {
		DEBUG and print "shift one task from queue, remain: ". $#{  $_[TX]->{taskq}  }."\n";
		if ($task->failed()) {
			carp('COMMIT: can\'t commit task '. $task->name().' because it is marked as failed: '.sprintf("0x%x",$task->state()));
			#carp("autorollback=".$_[TX]->{autorollback});
			$_[TX]->__rb();
			return wantarray?():undef;
		}
		my @rc = $_[TX]->__TX_run_task($task);
		my $chk_rc = $task->{'_rc'};
		my @args = ();
		if ( $#{$chk_rc} > 0 ) {
			@args = (@{$chk_rc}[1..$#{$chk_rc}],@rc)
		} else {
			@args = @rc;
		}
		if (!&{$chk_rc->[0]}( @args )) {
			carp('COMMIT: task '. $task->name() . 'failed to commit.');
			$task->mark_failed();
			push @{ $_[TX]->{rstack} }, $task;
			$_[TX]->{taskmap}->{$task->name()}->{'_run_output'} = \@rc;
			$_[TX]->__rb();
			return wantarray? () : undef;
		} else {
			#carp('COMMIT: task ' . $task->name() . ' COMMIT_OK.');
			$task->state(ST_TASK_CALLBACK);
			push @{ $_[TX]->{rstack} }, $task;
			$_[TX]->{taskmap}->{$task->name()}->{'_run_output'} = \@rc;
		}
	}
	if ($_[TX]->{protocol} == TX_ONE_PHASE_COMMIT) {  $_[TX]->{state} = ST_TX_FINI; $_[TX]->__TX_run_cb();}
	return 1;
}

sub results
{
	return $_[TX]->{taskmap}->{$_[1]}->{'_run_output'};
}

sub rollback
{
	my $rstack_top_idx = $#{  $_[TX]->{rstack}  };
	my $rb_start_from = $rstack_top_idx;
	if ($_[TX]->{rstack}->[$rstack_top_idx]->isa('Env::Transaction::AtomicTask') &&
		$_[TX]->{rstack}->[$rstack_top_idx]->failed()) {
			$rb_start_from = $rstack_top_idx - 1;
	}
	DEBUG and print STDERR "rstack_last_idx = ".$#{  $_[TX]->{rstack}  }.", rb_start_from=$rb_start_from\n";
	#my $rb_start_from = $_[TX]->{rstack}->[$rstack_top_idx]->isa('Env::Transaction::AtomicTask') 
	#	? $rstack_top_idx - 1 
	#	: $rstack_top_idx;
	for (my $i = $rb_start_from; $i >= 0;  $i--) {
		my $curtask = $_[TX]->{rstack}->[$i];
		DEBUG and __tx_debug($_[TX],$curtask,"rollback_task $curtask->{_name} ");
		next unless exists $curtask->{'_undo'}; #already checked this but maybe user hacked into taskq so foobit
		my $cmd = $curtask->{'_undo'};
		my @rc = ();
		## no critic
		eval { @rc = (&{$cmd->[0]}( ( $#{$cmd} > 0 ) ? (@{$cmd}[1..$#{$cmd}]): () ) ) ; };
		## use critic
		if ($@) { carp('UNEXPECTED_UNDO_ERROR'.$@); }
		$_[TX]->{taskmap}->{$curtask->name()}->{'_undo_output'} = \@rc;
		#$curtask->state(ST_TASK_UNDO);
	}
	return 1;
}

sub cleanup
{
	delete $_[TX]->{taskq};
	delete $_[TX]->{rstack};
	return 1;
}
#private method, don't call
sub __TX_run_cb
{
	if ($_[TX]->{state} != ST_TX_FINI) {
		return wantarray? () : undef;
	}
	foreach my $task (@{ $_[TX]->{rstack} }) {
	#while (my $task = shift @{ $_[TX]->{rstack} }) {
		DEBUG and __tx_debug($_[TX],$task,'callback_task');
		next unless exists $task->{'_callback'};
		my $cmd = $task->{'_callback'};
		my @rc = ();
		## no critic
		eval { @rc = (&{$cmd->[0]}( ( $#{$cmd} > 0 ) ? (@{$cmd}[1..$#{$cmd}]): () ) ) ; };
		## use critic
		if ($@) { carp('UNEXPECTED_CALLBACK_ERROR'.$@); }
		$_[TX]->{taskmap}->{$task->name()}->{'_callback_output'} = \@rc;
		$task->state(ST_TASK_OK_DONE);
	}
	return 1;
}
#private method, don't call
sub __TX_run_task
{
	#($tx,$task)
	my @rc = ();
	my $cmd = $_[TASK]->{'_run'};
	DEBUG and __tx_debug($_[TX],$_[TASK],'running_task');
	#print STDERR "__TX_run_task(".$_[TASK]->name()."->run(" . join(',',@$cmd) . "));\n";
	#print "BUAH: $#{@$cmd} ", join(',',@{$cmd}[1..$#{@$cmd}]),"\n";
	## no critic
	eval { @rc = (&{$cmd->[0]}( ( $#{$cmd} > 0 ) ? (@{$cmd}[1..$#{$cmd}]): () ) ) ; };
	## use critic
	if ($@) {
		carp($@);
		return wantarray? () : undef;
	}
	return @rc;
}

#private method, don't call
sub __TX_task_id 
{
	#my ($pkg,$fn,$line) = caller;
	#print "__TX_task_id call from line $line\n";
	return ( defined $_[ 2 ] )
	  ? $_[TX]->{taskmap}->{$_[TASK]->name()}->{id} = $_[ 2 ]
	  : $_[TX]->{taskmap}->{$_[TASK]->name()}->{id};
};

1;
__END__




=head1 NAME

Env::Transaction - Perl extension for running a set of operations 
within a safe, transactional execution model with either ONE- or TWO- phase 
protocols.

=head2 DON'T FORGET! checkout Env-Transaction/t/*.t files from distro dir


=head1 SYNOPSIS

  use Env::Transaction;
	sub Env::Transaction::DEBUG { 1; } #that if you want debug infos
	sub task_func { print "brain degeneration stage $_[0]\n"; return 1; }

	my $tx = new Env::Transaction(protocol=>TX_TWO_PHASE_COMMIT,autorollback=>1);
	my $task1 = new Env::Transaction::Task(
		name=>'task_ONE',
		run=>[\&task_func,'run',1],
		undo=>[\&task_func,'undo',1],
		callback=>[\&task_func,'callback',1],
		rc=>[\&task_func,'rc'],
	);
	#...
	#...
	$tx->prepare(
		$task1,
		new Env::Transaction::Task(
			name=>'task_TWO',
			#...
		)
	);
	$tx->commit();
	$tx->cleanup();




=head1 DESCRIPTION

Hint: I use this module for committing sets of system configurations to one or
more servers. This is not an ACID XA compatible transaction module but rather
a quick and !dirty way of saving yourself from cleaning up the mess left during 
a sysadmin fingerpanzer division assault.


=head1 METHODS

=head2 Env::Transaction::Task::new()

=head2 Env::Transaction::AtomicTask::new()

=head2 Env::Transaction::CheckpointTask::new()

creates a new task to be registered within a transaction
accepting the following parameters:

(

	name=>'taskname',
	run=>[\&coderef,@args],
	rc=>[\&coderef,@args],
	undo=>[\&coderef,@args],
	callback=>[\&coderef,@args],

)

an ::AtomicTask will have its undo method ignored if the task's run method fails.
That is, an ::AtomicTask can't modify anything outside unless it succeeds. However, if
an ::AtomicTask 'A' succeeds but a later task 'T' fails then 'A''s undo method will get to
be executed in the stack rundown process, ofcourse. Ideally there will be only ::AtomicTasks inside a transaction
but unfortunately (or fortunately) a normal ::Task can use its undo method to clean its 
mess on personal failure.

a ::CheckpointTask is just a ::Task that will only run in the callback stage

=head3 On 'new' arguments

the 'name' is mandatory.  you can later lookup return values of 'TASK_3::run' method
from within 'TASK22::callback' method with $tx->results('TASK_3','run')

'run' contains whatever operation you want to execute. 

'rc': you can specify a subroutine to check return codes of 'run'. it should return DEFINED
if 'run' returned what you expected

'undo': this is a subroutine that should undo whatever 'run' accomplished if a later task fails.
it is also used to clean its own mess if the task it is bound to isn't an ::AtomicTask. undo is
supposed to always SUCCEED (no failure expected)

'callback': this is a method that will be executed in a row (same order as the tasks were registered with)
after the entire transaction was committed successfully.  this will allow you to peek into tx stacks, clean
tasks mess, etc.

'name', 'run' and 'undo' are mandatory. If no 'rc' is specified then a default one will be used that 
will check for DEFINED return codes. I advise to use your own 'rc' checkers. 'callback's are optional.

'rc' will get the return values from 'run' appended to the parameters list.

'name', 'run', 'undo', 'rc' and 'callback' arguments are array references. the first element must always be
a coderef, the rest are (optional) parameters to be passed to the coderef on its execution. 


new methods will return undef on bad parameters.

=head2 Env::Transaction::new()

creates a new transaction. accepts the following key=>value parameters:

(

	protocol=>TX_TWO_PHASE_COMMIT | TX_ONE_PHASE_COMMIT
	autorollback=> BOOL_TRUE | BOOL_FALSE

)

=head3 transaction protocol

if TX_TWO_PHASE_COMMIT is specified then a task will be transparently committed 
when $tx->prepare($task) is called. A call of $tx->commit() will never fail.
Useful when you have a local master transaction and remote slave transactions and preparing tasks locally means 
committing tasks remotely. Or just when you dont want your $tx->commit() to fail but rather process the rollbacks
at prepare() time.

TX_ONE_PHASE_COMMIT will act normally, $tx->commit()-ing the tasks on explicit request.

autorollback is self-explanatory

=head2 Env::Transaction::prepare()

called as $tx->prepare($task1, ......, $taskN);

will register the task(s) within transaction $tx. if TWO-phase protocol was specified at $tx construction
then the tasks will also be committed

=head2 Env::Transaction::commit()

called as $tx->commit() with no args

will commit the registered tasks and will run the callbacks if any registered (in TWO-phase protocol mode
the callbacks will get to run when there is no pending transaction in the queue at the momment the commit() was
called).

=head2 Env::Transaction::cleanup()

$tx->cleanup()

gives you a chance to clean the task queue and the runstack after a commit()

=head2 Env::Transaction::id()

$tx->id() returns the transaction id. format: $tx+0.$$

=head2 Env::Transaction::results()

$tx->results('taskname')

will return an array with the return value(s) of 'run' command executed within 'taskname'. Note: you will always have
arrays as return values even if your method will return just a scalar - that will be coated with () by divine intervention



example: in your 'task3::callback' method { my $socket = ($tx->results('ssh_connect'))[1]; close($socket);}

don't forget callbacks are executed in the same order the tasks were registered (like 'run', un-like 'undo').


this  lets you chain the results from an earlier task to the current one.

=head1 TODO

a lot. consider this module to be in DEV stage altho the currently documented API will not change but more will be added
in time (e.g. clerks to control multiple inter-dependent transactions and policies to resolve dependencies)


=head2 EXPORT

Env::Transaction exports by default the constants TX_TWO_PHASE_COMMIT and TX_ONE_PHASE_COMMIT. The rest
are some sort of perl object-oriented subliminal techniques.

=head1 CAVEATS/BUGS

bugs - probably. code is a mess, please rely on pod only when using the module. this is the same with the virgin male to whom you give too much and too fast possey access. he will mess around with a lot of bloodshed and no victims ...  but will give you awesome war stories. thats what l.w. made with perl to all virgin perl codewriters. 

=head1 SEE ALSO

the pyramids.

=head1 AUTHOR

adrian ilarion ciobanu, E<lt>cia@mud.roE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2009 by adrian ilarion ciobanu

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.


=cut
