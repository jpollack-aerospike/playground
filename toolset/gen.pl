#!/usr/bin/perl

use strict;
use JSON;
use List::Util;
use Data::Dumper;
use File::Basename;
use Cwd 'abs_path';
# spaced out 1.3*e^n: 4, 10, 26, 71, 193.
# 24*3 ~~ 71, so we can use countries with 0, 1, 2
sub slurp {
    open (my $fh, '<', $_[0]) or die "open: $! $_[0]";
    local $/ = undef;
    return <$fh>;
}

my $json = JSON->new->utf8->canonical;
my $dr = (fileparse(abs_path(__FILE__)))[1];
my $defs = $json->decode (slurp ("${dr}defaults.json"));

die "usage: $0 STRUCTURE.JSON [SEED] [NUM_ITERATIONS]" unless ($#ARGV >= 0);
my $fname = shift;
my $seed = shift // 1;
my $niter = shift // 1;
srand($seed);

my $mmp = $json->decode(slurp ($fname));
my %mmpkbl = ();

for my $k (keys (%{$mmp})) {
    my $pa = [ split (/\./, $k) ];
    shift (@{$pa});
    my $pnw = [ map { (($_ eq "[") or ($_ eq "{")) ? 0 : 1 } @{$pa} ];
    push (@{$mmpkbl{"len_" . $#{$pa}}}, { parts => $pa, nw => $pnw, orig => $k });
}

sub find_best {
    my $input = shift;
    my $best_match = ".";
    my $best_score = -1;
    my $val = $mmpkbl{"len_" . $#{$input}};
    my @it = map { /^\d+$/ ? "[" : "{" } @{$input};
    for my $pd (@{$val}) {
	my $score = 0;
	for (my $i=0; ($i<=$#{$input} and ($score != -1)) ; $i++) {
	    $score = (($pd->{parts}->[$i] eq ($pd->{nw}->[$i] ? $input->[$i] : $it[$i]))  # if it's not a wildcard, exact match, if it is a wildcard type match
		      ? ($score + $pd->{nw}->[$i]) # increment score only on exact match
		      : -1); # not an exact match, or mismatch types
 	}
	($best_score, $best_match) = ($score, $pd->{orig}) if ($score > $best_score);
    }

    return $best_match;
}

sub mktyel {
    my ($p, $c) = @_;
    while (ref ($p) eq "ARRAY") {
	my ($rv, $cv, $ii) = (rand (List::Util::sum (map { $_->[0] } @{$p})), $p->[0]->[0], 0);
	while ($cv < $rv) { $cv += $p->[++$ii]->[0] ; }
	$p = $p->[$ii]->[1];
    }

    die unless (scalar (keys (%{$p})) == 1);
    my ($t, $a) = each %{$p};
    return $a if (ref ($a) ne "HASH");
    die "$t" unless (defined($defs->{$t}));
    my %aa = (%{$defs->{$t}}, %{$a});

    if ($t eq "int") {			return $aa{min} + int (rand ($aa{max} - $aa{min})); }
    elsif ($t eq "str") {		return join ('', map { chr(mktyel({int => $aa{chr}})) } (1..mktyel({int => $aa{len}}))); }
    elsif ($t eq "list") {		return [ map { mkelc ([ @{$c}, $_ ]) } (1..mktyel({int => $aa{len}})) ]; }
    elsif ($t eq "hash") {
	die unless (ref ($aa{keys}) eq "ARRAY");
	return { map { $_ => mkelc([ @{$c}, $_ ]) } @{$aa{keys}} };
    }
    else {				die "Type $t not found";    }
}

sub mkelc {
    my $c = shift;
    return mktyel ($mmp->{find_best($c)}, $c);
}

for (1..$niter) {
    print $json->encode (mkelc([])) . "\n";
}
