# $Id: Task.pm 1 2009-01-29 21:34:01Z ai-ciobanu $

package Env::Transaction::Task;
use 5.008008;
use strict;
#use warnings;
use Carp qw(croak carp);
use UNIVERSAL qw(isa);
use vars qw($VERSION);
our $VERSION = '0.999';
use Readonly;
#use base qw(Exporter);
require Exporter;
our @ISA = qw(Exporter);
#our %EXPORT_TAGS = ( 'constants' => [ qw(
#ST_TASK_PREPARE
#ST_TASK_COMMIT
#ST_TASK_UNDO
#ST_TASK_CALLBACK)
#]);
#
#our @EXPORT_OK = ( @{ $EXPORT_TAGS{'constants'} } );

sub ST_TASK_MARK_OK     { return 0x0001; }
sub ST_TASK_MARK_FAILED { return 0xffff; }
sub ST_TASK_PREPARE     { return 0x0010; }
sub ST_TASK_COMMIT      { return 0x0020; }
sub ST_TASK_UNDO        { return 0x0030; }
sub ST_TASK_CALLBACK    { return 0x0040; }
sub ST_TASK_OK_DONE { return 0x0010; }

our @EXPORT =qw(
ST_TASK_PREPARE
ST_TASK_COMMIT
ST_TASK_UNDO
ST_TASK_CALLBACK
ST_TASK_OK_DONE
);

#private method
sub __TASK_chk_ret
{
	if ($#_ < 0 || !defined $_[0]) { return wantarray? () : undef; }
	return 1;
}

sub new {
	shift;
	my %args = @_;
	if ( !defined $args{name} ) {
		carp('not going to register anonymous tasks');
		return wantarray ? () : undef;
	}
	my $task = {};
	Readonly::Scalar $task->{_name} => delete $args{name};
	if (!defined $args{'rc'} ) {
		$args{'rc'} = [\&__TASK_chk_ret];
	}
	if ( !defined $args{run} || !defined $args{undo} ) {
		carp('not going to use a task without a \'run\' or \'undo\' method');
		return wantarray ? () : undef;
	}
	foreach my $command (qw(run undo callback rc)) {
		if ( defined $args{$command} ) {
			if ( 'ARRAY' ne ref $args{$command} ) {
				carp("$command must be an array ref");
				return wantarray ? () : undef;
			}
			my $cmdargs = delete $args{$command};
			if ( 'CODE' ne ref $cmdargs->[0] ) {
				carp('first command argument must be a coderef');
				return wantarray ? () : undef;
			}
			$task->{"_$command"} = $cmdargs;
		}

	}
	$task->{_tx_task_state} = ST_TASK_MARK_OK;
	bless $task, __PACKAGE__;
	return $task;
}

sub name { return $_[0]->{_name}; }
sub state { return (defined $_[1])?$_[0]->{_tx_task_state} = $_[1] : $_[0]->{_tx_task_state} ; }
sub mark_failed { return $_[0]->{_tx_task_state} = ST_TASK_MARK_FAILED; }
sub mark_ok { return $_[0]->{_tx_task_state} = ST_TASK_MARK_OK; }
#sub ok { return ( ( $_[0]->state() & ST_TASK_MARK_FAILED ) == ST_TASK_MARK_FAILED ) ? 0 : 1; }
sub failed { $_[0]->state() == ST_TASK_MARK_FAILED  }
sub ok { return !$_[0]->failed(@_); }

1;
__END__

=head1 NAME

Env::Transaction::Task - define a task for using inside an Env::Transaction

=head1 SYNOPSIS

see Env::Transaction


=head1 SEE ALSO

Env::Transaction

=head1 AUTHOR

adrian ilarion ciobanu, E<lt>cia@mud.roE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2009 by adrian ilarion ciobanu

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.


=cut
