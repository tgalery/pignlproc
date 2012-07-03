#!/usr/bin/perl -w
#Author: Chris Hokamp
#for a TSV file, split and output 2 files up until $i = n (see loop below)
#one file is a list of surface forms, the other is a list of uris (these are ordered)

use strict;
use warnings;

#first get the URI names from sf_10.txt
my ($sf_10) = @ARGV;
open IN, "<", $sf_10;
open URI_OUT, ">", 'uri_list.txt';
open SF_OUT, ">", 'sf_list.txt';

#my $i = 0; #this is the number of sf --> {URI} groups to keep (start with 10)
while (my $line = <IN>)
{
	chomp ($line);
	#print "$line \n";
	my @elements = split(/\t/, $line);
	my $sf = $elements[0];
	#print "$sf \n";	
	print SF_OUT $sf."\n";
	my $uris = $elements[1];
	$uris =~ s/^\{//;
	$uris =~ s/}$//;
	my @uri_units = split (/,/, $uris);
	foreach my $uri (@uri_units)
	{
		$uri =~ s/^\(//;
		$uri =~ s/\)$//;
		#TEST
		print URI_OUT $uri."\n";
	}
	#$i++;
}

close IN;
close URI_OUT;
close SF_OUT;



