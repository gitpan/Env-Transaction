# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl Env-Transaction.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use Test::More tests => 9;
BEGIN { use_ok('Env::Transaction') };

sub Env::Transaction::DEBUG { 1 ; } 

sub task_func
{
	my ($taskname,$cmdtype,$failit) = @_;
	diag "task_func: $cmdtype on task $taskname\n";
	if ($failit) {
		return wantarray? () : undef;
	}
	return 1;
}

my $tx1 = new Env::Transaction ( protocol=>TX_ONE_PHASE_COMMIT );
my $tx2 = new Env::Transaction ( protocol=>TX_TWO_PHASE_COMMIT);
my $rc = undef;
diag("prepare four tasks that will succeed");
my @tasks = ();

for my $tname (qw(OK_ONE OK_TWO OK_THREE OK_FOUR)) {
	my $task = new Env::Transaction::Task(
		name		 => "task_$tname",
		run 		 => [ \&task_func, $tname, 'runthistask',		0],
		undo 		 => [ \&task_func, $tname, 'undothistask',	0],
		callback => [ \&task_func, $tname, 'callback', 			0],
		rc			 => [ \&task_func, $tname, 'check_runvals', 0],
	);
	push @tasks, $task;
	#isa_ok($task,'Env::Transaction::Task');
}
$rc = $tx1->prepare(@tasks);
cmp_ok($rc,'==',1, "tx " . $tx2->id() . "PREPARE_OK");
$rc = $tx1->commit();
cmp_ok($rc,'==',1,"tx " . $tx1->id() . " COMMIT_OK");


$rc = $tx2->prepare(@tasks);
cmp_ok($rc,'==',1, "tx ". $tx1->id() . " PREPARE_OK");
$rc = $tx2->commit();
cmp_ok($rc,'==',1,"tx " . $tx2->id() . " COMMIT_OK");

undef $tx1;
undef $tx2;
undef @tasks;



$tx1 = new Env::Transaction ( protocol=>TX_ONE_PHASE_COMMIT, autorollback=>1 );
$tx2 = new Env::Transaction ( protocol=>TX_TWO_PHASE_COMMIT);
$rc = undef;
diag("prepare four tasks, third one will fail");
@tasks = ();
for my $tname (qw(OK_FIVE OK_SIX FAIL_SEVEN OK_EIGHT)) {
	my $task = new Env::Transaction::Task(
		name		 => "task_$tname",
		run 		 => [ \&task_func, $tname, 'runthistask',		($tname eq 'FAIL_SEVEN')?1:0],
		undo 		 => [ \&task_func, $tname, 'undothistask',	0],
		callback => [ \&task_func, $tname, 'callback', 			0],
		rc			 => [ \&task_func, $tname, 'check_runvals', ($tname eq 'FAIL_SEVEN')?1:0],
	);
	push @tasks, $task;
	#isa_ok($task,'Env::Transaction::Task');
}

$rc = $tx1->prepare(@tasks);
#is($rc,undef,"task_$tname PREPARE_WILL_FAIL");
cmp_ok($rc,'==',1, "tx " . $tx2->id() . " PREPARE_OK");
$rc = $tx1->commit();
#cmp_ok($rc,'==',1,"tx " . $tx1->id() . " COMMIT_OK");
is($rc,undef,"COMMIT_WILL_FAIL");

diag("will fail to prepare a task marked as failed in a two-phase tx");
$rc = $tx2->prepare(@tasks);
is($rc,undef,"PREPARE_WILL_FAIL");
$rc = $tx2->rollback();
cmp_ok($rc,'==',1,"tx " . $tx2->id() . " ROLLBACK_STAGE_OK");
#use Data::Dumper;
#print Dumper($tx2);
#cmp_ok($rc,'==',1, "task_$tname PREPARE_OK");
#$rc = $tx2->commit();
#cmp_ok($rc,'==',1,"tx " . $tx2->id() . " COMMIT_OK");

#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.

