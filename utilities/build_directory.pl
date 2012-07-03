#!/usr/bin/perl -w
#Author: Chris Hokamp
#for a TSV file, split and output 1 file with resource name = file name; contents = text

use strict;
use warnings;

#first get the URI names from sf_10.txt
my $tsv_file = $ARGV[0];
open IN, "<", $tsv_file;

while (my $line = <IN>)
{
	chomp ($line);
	my @elements = split(/\t/, $line);
	my $uri = $elements[0];
	$uri =~ /.*\/([^\/]*)/;
	my $filename = $1;
	#print "$uri\n";
	#print "$filename\n";
	my $context = $elements[1];
	#print "\n$context\n";
	open OUT, ">", "$filename" or die $!;
	print OUT $context;
	close OUT;
}



