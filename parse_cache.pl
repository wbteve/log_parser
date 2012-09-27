#!/usr/bin/perl

use MongoDB;
use Date::Parse;
use Scalar::Util qw(looks_like_number);
use Parallel::ForkManager;
use Data::Dumper;
use Time::HiRes qw(gettimeofday tv_interval);

# 91.222.220.245 - - [02/Aug/2012:01:10:21 +0400] "GET http://player.rutv.ru/index/play/cast_id/6125/1343855415000 HTTP/1.1" 200 376 "http://player.rutv.ru/flash/skin/livedefault.swf?v=63" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; InfoPath.2; .NET4.0C; .NET4.0E; MS-RTC LM 8)" 0.148 0.006 : 0.142 "192.168.10.69:11211 : 192.168.10.254:86" MISS "404 : 200" "player.rutv.ru/index/play/cast_id/6125/country/UA/"
# 46.146.3.130 - - [02/Aug/2012:01:10:20 +0400] "GET http://player.rutv.ru/index/config/sid/mini HTTP/1.1" 200 437 "http://player.rutv.ru/flash/flvplayer_videoHost.swf?v=63" "Opera/9.80 (Windows NT 6.1; WOW64; U; Edition Yx; ru) Presto/2.10.289 Version/12.00" 0.008 0.008 "192.168.10.69:11211" - "200" "player.rutv.ru/index/config/sid/mini"

my $DEBUG=0;
my $NEW_LAST=1;

my %fhs;

print_log("Log parser started");

if ($DEBUG){
	main();
}
else{
	while (-e 'run_parser'){
		main();
		sleep(5);
	}
}

print_log("Log parser exit");

exit;

my %f_cache;

sub main{
	my $files=get_list();
	print_log(scalar(@$files).' files found') if $DEBUG;
	my $m1 = MongoDB::Connection->new(host => '192.168.10.16', port => 27017);
	$MongoDB::BSON::looks_like_number = 1;
	my $db1 = $m1->logs_files;
	my $c_files = $db1->files;
	my $sum_delta=0;
	my @proc_list;
	for my $f (@$files){
		unless ($f_cache{$f}){
			open($f_cache{$f},$f);
		}
		my $c_info=$c_files->find({'filename'=>$f});
		my $info;
		my $size=-s $f;
		if ($c_info->count()){
			$info=$c_info->next();
			my $delta=$size-$info->{last_tell};
			print_log($delta.' bytes in '.$f) if ($delta and $DEBUG);
		}
		else{
			my %inf;
			$inf{filename}=$f;
			$inf{last_size}=$size;
			if ($NEW_LAST){
				$inf{last_tell}=$inf{last_size};
			}
			else{
				$inf{last_tell}=0;
			}
			$c_files->insert(\%inf);
			$info=\%inf;
		}
		if ($size<$info->{last_size}){
			close($f_cache{$f},$f);
			open($f_cache{$f},$f);
			$info->{last_size}=$size;
			$info->{last_tell}=0;
		}
		if ($size-$info->{last_tell} > 100){
			$info->{last_size}=$size;
			$sum_delta += ($size-$info->{last_tell});
			print_log("Candidate to processing: $f") if $DEBUG;
			push(@proc_list,$info);
		}
	}
	my $pm = new Parallel::ForkManager(5);
	print_log("Going to process $sum_delta bytes in ".(scalar(@proc_list))." files");
	for my $p (@proc_list){
		$pm->start and next;
		parse_file($p);
		$pm->finish;
	}
	$pm->wait_all_children;
	print_log("End Processing");
#	$m1->logs2->lines->remove({tstamp=>{'$lt'=>time-5*60}});
#	$m1->logs2->lines->remove({tstamp=>'null'});
}

my $reg_time=0;
my $mongo_time=0;

sub parse_file{
	my ($inf)=@_;
	print_log("Start processing ".$inf->{filename}) if $DEBUG;
	my $m2 = MongoDB::Connection->new(host => '192.168.10.16', port => 27017,max_bson_size=>67108864);
	$MongoDB::BSON::looks_like_number = 1;
	my $db2 = $m2->logs_files;
	my $files = $db2->files;
	my $db3 = $m2->logs2;
	my $lines = $db3->lines;
	my $source='unknown';
	if ($inf->{filename}=~/nginx0/){
		$source='nginx0';
	}
	if ($inf->{filename}=~/nginx1/){
		$source='nginx1';
	}
	if ($inf->{filename}=~/nginx2/){
		$source='nginx2';
	}
	if ($inf->{filename}=~/nginx4/){
		$source='nginx4';
	}

	seek ($f_cache{$inf->{filename}},$inf->{last_tell},0);
	my $lns=0; 
	my @push_lines;
	my $ff=$f_cache{$inf->{filename}};
	while (my $l = <$ff>){
		if ((chomp $l) and ($inf->{last_size}>$inf->{last_tell})){
			my $pl=parse_line($l,$source);
			for my $p (@$pl){
				push (@push_lines,$p);
			}
			$inf->{last_tell}=tell($ff);
			$lns++;
		}
		if ($lns>100){
			$lines->batch_insert(\@push_lines);
			@push_lines=();
			$lns=0;
		}
	}
	if ($push_lines[0]){
		$lines->batch_insert(\@push_lines);
	}
	$files->update({"filename"=>$inf->{filename}},{'$set'=>{"last_tell"=>$inf->{last_tell}}});
	$files->update({"filename"=>$inf->{filename}},{'$set'=>{"last_size"=>$inf->{last_size}}});
	print_log("Finish processing ".$inf->{filename}) if $DEBUG;
}


sub print_lines{
	my ($pl)=@_;
	for my $l (@$pl){
		my $f;
		if ($fhs{$l->{tstamp}}){
			$f=$fhs{$l->{tstamp}};
		}
		else{
			open ($f,'>>data_imp/'.$l->{tstamp});
			$fhs{$l->{tstamp}}=$f;
		}
		print $f $l->{tstamp}.',"'.$l->{ip}.'","'.$l->{user}.'","'.$l->{date}.'","'.$l->{time}.'","'.$l->{method}.'","'.$l->{url}.'","'.$l->{version}.'",'.$l->{code}.','.$l->{size}.',"'.$l->{ref}.'","'.$l->{agent}.'",'.$l->{resp_time}.','.$l->{upstream_time}.',"'.$l->{upstream}.'","'.$l->{cache_status}.'","'.$l->{upstream_code}.'","'.$l->{memcache_key}.'","'.$l->{backend}.'","'.$l->{resp_compl}.'","'.$l->{content_type}.'","'.$l->{request_id}.'","'.$l->{uid_got}.'","'.$l->{uid_set}.'","'.$l->{frontend}.'","'.$l->{host}."\"\n";
	}
}

sub print_log{
	for my $e (@_){
		print STDERR localtime()."\t$e\n";
	}
}

sub get_list{
	my @list;
	for my $d qw(/mnt/nfs-logs/nginx0 /mnt/nfs-logs/nginx1 /mnt/nfs-logs/nginx2 /mnt/nfs-logs/nginx4){
		opendir (D1,$d);
		while (my $f1=readdir(D1)){
			if (($f1 =~ /.*access.log$/) or ($f1 eq 'access_custom.log')){
				push(@list,$d.'/'.$f1);
			}
		}
		closedir D1;
	}
	return \@list;
}

sub parse_line{
	my ($l,$source)=@_;
	my $inf;
	my $to=[gettimeofday];
	#$l =~ /$rr/gi;
	($inf{ip},$inf{user},$inf{date_time_tz},$inf{method},$inf{url},$inf{version},$inf{code},$inf{size},$inf{ref},$inf{agent},$inf{resp_time},$inf{upstream_time},$inf{upstream},$inf{cache_status},$inf{upstream_code},$inf{memcache_key},$inf{enhanced_memcache_key},$inf{backend},$inf{resp_compl},$inf{content_type},$inf{request_id},$inf{uid_got},$inf{uid_set},$hz)=split('    ',$l);
	$reg_time += tv_interval($to);
	$inf{resp_time}=looks_like_number($inf{resp_time})?($inf{resp_time}*1000):-1;
	($inf{date_time},$hz)=split(' ',$inf{date_time_tz});
	$inf{tstamp}=str2time($inf{date_time});
	$inf{frontend}=$source;
	$inf{type}='nginx';
	$inf{url}=~ /http:\/\/(.*?)$/gi;
	my $t11=$1;
	my ($host,$other)=split('/',$t11);
	$inf{host}=($host)?$host:$t11;
	my @up_time;
	my @up;
	my @up_code;
	if ($inf{upstream} =~ / : /){
		@up_time=split(' : ',$inf{upstream_time});
		@up=split(' : ',$inf{upstream});
		@up_code=split(' : ',$inf{upstream_code});
	}
	else{
		@up_time=($inf{upstream_time});
		@up=($inf{upstream});
		@up_code=($inf{upstream_code});
	}
	my @pl;
	for (my $i=0;$i<$#up+1;$i++){
		$inf{upstream_time}=looks_like_number($up_time[$i])?($up_time[$i]*1000):-1;
		$inf{upstream}=$up[$i];
		$inf{upstream_code}=$up_code[$i];
		$inf{gf}=join(' ',($inf{upstream},$inf{code},$inf{host},$inf{backend},$inf{frontend},$inf{cache_status},$inf{content_type},$inf{url},$inf{upstream_code}));
		my %in;
		%in=%inf;
		push (@pl,\%in);
	}
	return \@pl;
}

1;
