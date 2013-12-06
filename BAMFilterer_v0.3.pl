#!/usr/bin/perl -w

#====================================================================================================================================================#
#<use>
$|++; #---turn on the auto flush for the progress bar
use strict;
use File::Path;
use Time::HiRes qw( time );
use Storable;
use Getopt::Long;
use File::Basename;
use File::Spec::Functions qw(rel2abs);
use List::Util qw (sum shuffle);
use threads;
use threads::shared;
use Statistics::Descriptive;
use Cwd 'abs_path';
#<\use>
#====================================================================================================================================================#

#====================================================================================================================================================#
#<doc>
#	Description
#		This is a perl script to filter the BAM file based on a range of hard coded criteria.
#
#	Input
#		--BAMPath=				file path; compulsory; Path of a BAM file;
#		--outDir=				dir path; output dir;
#
#	Usage
#		
#		perl BAMFilterer_v0.1.pl --BAMPath=/Volumes/A_MPro2TB/NGS/results/1301_sizeFraction100To500_test/tophat/allNoSec.bam --outDir=
#
#	Assumption
#
#	History:
#		
#		v0.1
#		-debut
#
#		v0.2
#		-rewritten in its simplest form, only accepts hard coded parameters.
#		[Sat  3 Aug 2013 13:32:50 CEST] added remove redundant
#
#		v0.3
#		[Sat 24 Aug 2013 13:49:29 CEST] cleaned with perlScriptCleaner
#<\doc>
#====================================================================================================================================================#

#====================================================================================================================================================#
#<global>
my $scriptDirPath = dirname(rel2abs($0));
my $globalTmpLogPath = "$scriptDirPath/tmp.log.txt";
open TMPLOG, ">", $globalTmpLogPath;
#<\global>
#====================================================================================================================================================#

#====================================================================================================================================================#
{	#Main sections lexical scope starts
#====================================================================================================================================================#

#====================================================================================================================================================#
#	section 0_startingTasks
#	primaryDependOnSub: checkSamtoolsVersion|141, printCMDLogOrFinishMessage|175, readParameters|340
#	secondaryDependOnSub: currentTime|160, reportStatus|371
#
#<section ID="startingTasks" num="0">
########################################################################## 
&printCMDLogOrFinishMessage("CMDLog");#->175
my ($BAMPath, $outDir) = &readParameters();#->340
&checkSamtoolsVersion();#->141
#<\section>
#====================================================================================================================================================#

#====================================================================================================================================================#
#	section 1_defineHardCodedParam
#	primaryDependOnSub: >none
#	secondaryDependOnSub: >none
#
#<section ID="defineHardCodedParam" num="1">
########################################################################## 
my $hardCodedParamHsh_ref = {};
$hardCodedParamHsh_ref->{'minLength'} = 32;
$hardCodedParamHsh_ref->{'maxNM'} = 2;
$hardCodedParamHsh_ref->{'maxNH'} = 100;
$hardCodedParamHsh_ref->{'min5EndMatchBlock'} = 0;
$hardCodedParamHsh_ref->{'min3EndMatchBlock'} = 8;
$hardCodedParamHsh_ref->{'removeRedundant'} = 'no'; #----remove redundant reads,defined as reads as start at the same position with the same sequences;
#<\section>
#====================================================================================================================================================#

#====================================================================================================================================================#
#	section 2_processTheBAM
#	primaryDependOnSub: readAndFilterBAMOnTheFly|207
#	secondaryDependOnSub: checkBAMReadNumber|121, reportStatus|371
#
#<section ID="processTheBAM" num="2">
########################################################################## 
my ($outBAMPath) = &readAndFilterBAMOnTheFly($BAMPath, $outDir, $hardCodedParamHsh_ref);#->207
system "samtools index $outBAMPath";
#<\section>
#====================================================================================================================================================#

#====================================================================================================================================================#
#	section 3_finishingTasks
#	primaryDependOnSub: printCMDLogOrFinishMessage|175
#	secondaryDependOnSub: currentTime|160
#
#<section ID="finishingTasks" num="3">
########################################################################## 
&printCMDLogOrFinishMessage("finishMessage");#->175
close TMPLOG;
#<\section>
#====================================================================================================================================================#

#====================================================================================================================================================#
}	#Main sections lexical scope ends
#====================================================================================================================================================#

sub checkBAMReadNumber {
#....................................................................................................................................................#
#	dependOnSub: reportStatus|371
#	appearInSub: readAndFilterBAMOnTheFly|207
#	primaryAppearInSection: >none
#	secondaryAppearInSection: 2_processTheBAM|93
#	input: $BAMPath
#	output: $totalReadNum
#	toCall: my ($totalReadNum) = &checkBAMReadNumber($BAMPath);
#	calledInLine: 239
#....................................................................................................................................................#

	my ($BAMPath) = @_;
	
	&reportStatus("Checking BAM file size", 10, "\n");#->371
	my $flagStatOut = `samtools flagstat $BAMPath`;
	my ($totalReadNum) = split / /, $flagStatOut;

	return $totalReadNum;
}
sub checkSamtoolsVersion {
#....................................................................................................................................................#
#	dependOnSub: reportStatus|371
#	appearInSub: >none
#	primaryAppearInSection: 0_startingTasks|63
#	secondaryAppearInSection: >none
#	input: none
#	output: none
#	toCall: &checkSamtoolsVersion();
#	calledInLine: 71
#....................................................................................................................................................#

	my $samtoolsStdout = `samtools 2>&1`;
	if ($samtoolsStdout =~ m/\s+(Version: \S+)\s+/) {
		&reportStatus("Checking: samtools: $1", 10, "\n");#->371
	} else {
		die "samtools not installed properly. Quitting.\n";
	}
}
sub currentTime {
#....................................................................................................................................................#
#	dependOnSub: >none
#	appearInSub: printCMDLogOrFinishMessage|175, reportStatus|371
#	primaryAppearInSection: >none
#	secondaryAppearInSection: 0_startingTasks|63, 3_finishingTasks|105
#	input: none
#	output: $runTime
#	toCall: my ($runTime) = &currentTime();
#	calledInLine: 194, 197, 202, 386
#....................................................................................................................................................#
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time);
	my $runTime = sprintf "%04d-%02d-%02d %02d:%02d", $year+1900, $mon+1,$mday,$hour,$min;	
	return $runTime;
}
sub printCMDLogOrFinishMessage {
#....................................................................................................................................................#
#	dependOnSub: currentTime|160
#	appearInSub: >none
#	primaryAppearInSection: 0_startingTasks|63, 3_finishingTasks|105
#	secondaryAppearInSection: >none
#	input: $CMDLogOrFinishMessage
#	output: none
#	toCall: &printCMDLogOrFinishMessage($CMDLogOrFinishMessage);
#	calledInLine: 69, 111
#....................................................................................................................................................#

	my ($CMDLogOrFinishMessage) = @_;
	
	if ($CMDLogOrFinishMessage eq "CMDLog") {
		#---open a log file if it doesnt exists
		my $absoluteScriptPath = abs_path($0);
		my $dirPath = dirname(rel2abs($absoluteScriptPath));
		my ($scriptName, $callScriptPath, $scriptSuffix) = fileparse($absoluteScriptPath, qr/\.[^.]*/);
		open (CMDLOG, ">>$dirPath/$scriptName.cmd.log.txt"); #---append the CMD log file
		print CMDLOG "[".&currentTime()."]\t"."$dirPath/$scriptName$scriptSuffix ".(join " ", @ARGV)."\n";#->160
		close CMDLOG;
		print "\n=========================================================================\n";
		print "[".&currentTime()."] starts running ...... \n";#->160
		print "=========================================================================\n\n";

	} elsif ($CMDLogOrFinishMessage eq "finishMessage") {
		print "\n=========================================================================\n";
		print "[".&currentTime()."] finished running .......\n";#->160
		print "=========================================================================\n\n";
	}
}
sub readAndFilterBAMOnTheFly {
#....................................................................................................................................................#
#	dependOnSub: checkBAMReadNumber|121, reportStatus|371
#	appearInSub: >none
#	primaryAppearInSection: 2_processTheBAM|93
#	secondaryAppearInSection: >none
#	input: $BAMPath, $hardCodedParamHsh_ref, $outDir
#	output: $outBAMPath
#	toCall: my ($outBAMPath) = &readAndFilterBAMOnTheFly($BAMPath, $outDir, $hardCodedParamHsh_ref);
#	calledInLine: 99
#....................................................................................................................................................#

	my ($BAMPath, $outDir, $hardCodedParamHsh_ref) = @_;

	my $minLength = $hardCodedParamHsh_ref->{'minLength'};
	my $maxNM = $hardCodedParamHsh_ref->{'maxNM'};
	my $maxNH = $hardCodedParamHsh_ref->{'maxNH'};
	my $min5EndMatchBlock = $hardCodedParamHsh_ref->{'min5EndMatchBlock'};
	my $min3EndMatchBlock = $hardCodedParamHsh_ref->{'min3EndMatchBlock'};
	my $removeRedundant = $hardCodedParamHsh_ref->{'removeRedundant'};

	#---get the outBAm path
	my ($BAMName,$BAMDir,$BAMSuffix) = fileparse($BAMPath, qr/\.[^.]*/);
	my $paramTag = "ML$minLength.NM$maxNM.NH$maxNH.5N$min5EndMatchBlock.3N$min3EndMatchBlock.RD$removeRedundant";
	my $outBAMPath = "$outDir/$BAMName.$paramTag.bam";

	open BAMOUT, "| samtools view -S -b - >$outBAMPath 2>/dev/null";

	#---print the header
	open BAMHEADER, "samtools view -H $BAMPath |";
	print BAMOUT while <BAMHEADER>; 
	close BAMHEADER;
	
	my $totalReadNum = &checkBAMReadNumber($BAMPath);#->121
	my $procRead = my $passedRead = 0;
	my $tmpRedundantPosSeqHsh_ref = {};
	my $lastReadStart = -1;
	
	open BAMIN, "samtools view $BAMPath |";
	while (my $theLine = <BAMIN>) {
		
		$procRead++;
		my ($rdName, $flag, $cntg, $curntReadStart, $mapQ, $cigarStr, $cntgNext, $posNext, $tLen, $readSeq, $qual) = split /\t/, $theLine;
		
		if (($procRead % 100000 == 0) or ($procRead == $totalReadNum)) {
			my $passedReadPct = sprintf "%.5f", 100*$passedRead/$procRead;
			my $procReadPct = sprintf "%.5f", 100*$procRead/$totalReadNum;
			&reportStatus("$passedReadPct\% passed\t$procReadPct\% processed", 20, "\r");#->371
		}

		####################
		# Check length
		####################
		my $length = length $readSeq;
		next if $length < $minLength;
		
		
		####################
		# Check NM
		####################
		my $NM = 0;
		$NM = $1 if ($theLine =~ m/\tNM:i:(\d+)\t/);
		next if $NM > $maxNM;
		

		####################
		# Check NH
		####################
		my $NH = 1;
		$NH = $1 if ($theLine =~ m/\tNH:i:(\d+)\t/);
		next if $NH > $maxNH;
		

		####################
		# Check endBlock Match
		####################
		my $readStrand = '+';
		$readStrand = '-' if ($flag & 16);###---reference: http://seqanswers.com/forums/showthread.php?t=2301
		if ($NM > 0) {
			my $MD = $1 if ($theLine =~ m/\tMD:Z:(\S+)\t/);
			my @MDSplt = split /\D+/, $MD;
			my ($match5EndBlock, $match3EndBlock);
			if ($readStrand eq '+') {
				($match5EndBlock, $match3EndBlock) = ($MDSplt[0], $MDSplt[-1]);
			} else {
				($match5EndBlock, $match3EndBlock) = ($MDSplt[-1], $MDSplt[0]);
			}
			next if $match5EndBlock < $min5EndMatchBlock or $match3EndBlock < $min3EndMatchBlock;
		}
		

		####################
		# Print the line if it passes all test
		####################
		if ($removeRedundant eq 'no') {
			print BAMOUT $theLine;
			$passedRead++;

		} else {

			#---store the read for later print when change readStartPos
			$tmpRedundantPosSeqHsh_ref->{$curntReadStart}{$readStrand}{$readSeq} = $theLine;

			#---print only if change readStartPos or end of file (assuming the bam are sorted)
			if (($curntReadStart ne $lastReadStart and $lastReadStart != -1) or (eof BAMIN)) {
				#---print the lastReadPos in any case
				my @posToPrintAry = ($lastReadStart);
				
				#---add the curntReadStart if eof and lastReadStart is not curntReadStart
				push @posToPrintAry, $curntReadStart if ((eof BAMIN) and ($curntReadStart ne $lastReadStart));

				foreach my $pos (@posToPrintAry) {
					foreach my $readStrand (keys %{$tmpRedundantPosSeqHsh_ref->{$pos}}) {
						foreach my $readSeq (keys %{$tmpRedundantPosSeqHsh_ref->{$pos}{$readStrand}}) {
							print BAMOUT $tmpRedundantPosSeqHsh_ref->{$pos}{$readStrand}{$readSeq};
							$passedRead++;
						}
					}
				}
				delete $tmpRedundantPosSeqHsh_ref->{$lastReadStart};
			}
			
			#---last change to curnt
			$lastReadStart = $curntReadStart;
		}
	}
	close BAMOUT;
	
	print "\n";
	
	return $outBAMPath;
}
sub readParameters {
#....................................................................................................................................................#
#	dependOnSub: >none
#	appearInSub: >none
#	primaryAppearInSection: 0_startingTasks|63
#	secondaryAppearInSection: >none
#	input: none
#	output: $BAMPath, $outDir
#	toCall: my ($BAMPath, $outDir) = &readParameters();
#	calledInLine: 70
#....................................................................................................................................................#
	
	my ($BAMPath, $outDir);
	
	my $dirPath = dirname(rel2abs($0));
	$outDir = "$dirPath/BAMFilterer/";

	GetOptions 	("BAMPath=s" => \$BAMPath,
				 "outDir:s"  => \$outDir)

	or die		("Error in command line arguments\n");
	
	#---check file
	foreach my $fileToCheck ($BAMPath) {
		die "Can't read $fileToCheck" if not -s $fileToCheck;
	}

	system "mkdir -p -m 777 $outDir/";
	
	return($BAMPath, $outDir);
}
sub reportStatus {
#....................................................................................................................................................#
#	dependOnSub: currentTime|160
#	appearInSub: checkBAMReadNumber|121, checkSamtoolsVersion|141, readAndFilterBAMOnTheFly|207
#	primaryAppearInSection: >none
#	secondaryAppearInSection: 0_startingTasks|63, 2_processTheBAM|93
#	input: $lineEnd, $message, $numTrailingSpace
#	output: 
#	toCall: &reportStatus($message, $numTrailingSpace, $lineEnd);
#	calledInLine: 134, 154, 253
#....................................................................................................................................................#
	my ($message, $numTrailingSpace, $lineEnd) = @_;

	my $trailingSpaces = '';
	$trailingSpaces .= " " for (1..$numTrailingSpace);
	
	print "[".&currentTime()."] ".$message.$trailingSpaces.$lineEnd;#->160

	return ();
}

exit;
