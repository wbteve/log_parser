#!/usr/bin/perl

use MongoDB;
use Data::Dumper;
use sort '_quicksort';

my $slave=MongoDB::Connection->new(host=>'192.168.10.16');
my $master=MongoDB::Connection->new(host=>'192.168.10.16');

my $lines=$slave->logs2->lines;

#my $last=$master->analytics->proc->find()->next->{last};
unless ($last){
	$last=time()-180;
	$master->analytics->proc->insert({last=>$last});
}
if ($last<time()-160)
{
	$last=time()-150;
}
while(1){
	my $new_last=time()-150;
	for (my $i=$last;$i<$new_last;$i=$i+1){
		my %agg;
		$agg{tstamp}=$i;
		my $res3=$slave->logs2->run_command(
			{
				aggregate=>'lines',
				pipeline=>[
					{'$match'=>{'type'=>'nginx','tstamp'=>{'$lt'=>$i,'$gte'=>$i-1}}},
					{'$project'=>{
#							'mmm'=>{
#								'$add'=>[
#									'$upstream',
#									' ',
#									'$code',
#									' ',
#									'$host',
#									' ',
#									'$backend',
#									' ',
#									'$frontend',
#									' ',
#									'$cache_status',
#									' ',
#									'$content_type',
#									' ',
#									'$url',
#									' ',
#									'$upstream_code'
#								]
#							},
							'mmm'=>'$gf',
							'upstream_time'=>1
							,'resp_time'=>1
							,'size'=>1
						}
					},
					{'$group'=>{'_id'=>'$mmm',total=>{'$sum'=>1},traffic=>{'$sum'=>'$size'},'times'=>{'$push'=>'$upstream_time'},'response'=>{'$push'=>'$resp_time'}}}
				]
			}
		);
		$rr3=$res3->{result};
		my @times_memcache;
		my @times_86;
		my @resp;
		my @resp_html;
		my @resp_json;
		my @resp_vesti;
		my @upstr_vesti;
		my @resp_player;
		my @upstr_player;
		my @resp_russia;
		my @upstr_russia;
		my @resp_russia2;
		my @upstr_russia2;
		for my $r (@$rr3){
			my ($upstream,$code,$host,$backend,$frontend,$cache_status,$content_type,$url,$upstream_code)=split(' ',$r->{_id});
			$agg{'over_'.$backend}+=$r->{total};
			$agg{'over_'.$frontend}+=$r->{total};
			$agg{over_front}+=$r->{total};
			$agg{traffic}+=$r->{traffic};
			my $rrr=$r->{response};
			my $uuu=$r->{times};
			@resp=(@resp,@$rrr);	
			if ($host =~ /russia.tv/){
				@resp_russia=(@resp_russia,@$rrr);
				if ($upstream =~ /86/){
					@upstr_russia=(@upstr_russia,@$uuu);	
				}
				if ($cache_status eq 'HIT'){
					$agg{site_russia_cache}+=$r->{total};
				}
				if ($upstream =~ /11211/ and $upstream_code eq '200'){
					$agg{site_russia_memcache}+=$r->{total};
				}
				if ($content_type =~ /html/){
					$agg{site_russia_text}+=$r->{total};
				}
				$agg{site_russia_hits}+=$r->{total};
				$agg{'site_russia_'.$code}+=$r->{total};
			}
			if ($host =~ /russia2.tv/){
				@resp_russia2=(@resp_russia2,@$rrr);
				if ($upstream =~ /86/){
					@upstr_russia2=(@upstr_russia2,@$uuu);	
				}
				if ($cache_status eq 'HIT'){
					$agg{site_russia2_cache}+=$r->{total};
				}
				if ($upstream =~ /11211/ and $upstream_code eq '200'){
					$agg{site_russia2_memcache}+=$r->{total};
				}
				if ($content_type =~ /html/){
					$agg{site_russia2_text}+=$r->{total};
				}
				$agg{site_russia2_hits}+=$r->{total};
				$agg{'site_russia2_'.$code}+=$r->{total};
			}
			if ($host eq 'player.rutv.ru'){
				@resp_player=(@resp_player,@$rrr);
				if ($upstream =~ /86/){
					@upstr_player=(@upstr_player,@$uuu);	
				}
				if (($cache_status eq 'HIT') or ($upstream =~ /11211/ and $upstream_code eq '200') ){
					$agg{site_player_cache}+=$r->{total};
				}
				$agg{site_player_hits}+=$r->{total};
				$agg{'site_player_'.$code}+=$r->{total};
			}
                        if ($host eq 'www.vesti.ru'){
                                @resp_vesti=(@resp_vesti,@$rrr);
                                if ($upstream =~ /82/){
                                        @upstr_vesti=(@upstr_vesti,@$uuu);
                                }
                                $agg{site_vesti_hits}+=$r->{total};
                                $agg{'site_vesti_'.$code}+=$r->{total};
				if ($url =~ /search/){
					$agg{site_vesti_search}+=$r->{total};
				}
				if ($cache_status eq 'HIT'){
					$agg{site_vesti_cache_hit}+=$r->{total};
				}
				if ($code =~ /^50/){
					$agg{site_vesti_50x}+=$r->{total};
				}
				if ($url =~ /404.html/ or $code eq '404'){
					$agg{'site_vesti_404'}+=$r->{total};
#					open (L404,'>>vesti_404.txt');
#					print L404 $url."\n";
#					close L404;
				}
                        }
			elsif ($url =~ /search/){
				if ($host =~ /mayak/){
					$agg{site_mayak_search}+=$r->{total};
				}
				elsif ($host =~ /tvkultura/){
					$agg{site_tvkultura_search}+=$r->{total};
				}
				elsif ($host =~ /russia.tv/){
					$agg{site_russia1_search}+=$r->{total};
				}
				elsif ($host =~ /russia2/){
					$agg{site_russia2_search}+=$r->{total};
				}
				elsif ($host =~ /rutv/){
					$agg{site_rutv_search}+=$r->{total};
				}
				elsif ($host =~ /vesti/){
					$agg{site_vesti_search}+=$r->{total};
				}
				else{
					$agg{site_other_search}+=$r->{total};
				}

			}
			if ($content_type =~ /html/){
				@resp_html=(@resp_html,@$rrr);
			}
			if ($content_type =~ /json/){
				@resp_json=(@resp_json,@$rrr);
			}
			if ($upstream =~ /11211/){
				$agg{over_memcache}+=$r->{total};
				if ($code eq '200'){
					$agg{memcache_200}+=$r->{total};
					if ($host =~ /russia.tv/){
						$agg{memcache_rus_200}+=$r->{total};
					}
				}
				if ($code eq '404'){
					$agg{memcache_404}+=$r->{total};
					if ($host =~ /russia.tv/){
						$agg{memcache_rus_404}+=$r->{total};
					}
				}
				@times_memcache=(@times_memcache,@$uuu);
			}
			if ($upstream =~ /82/){
				$agg{over_82}+=$r->{total};
			}
			if ($upstream =~ /83/){
				$agg{over_83}+=$r->{total};
			}
			if ($upstream =~ /85/){
				$agg{over_85}+=$r->{total};
			}
			if ($upstream =~ /86/){
				$agg{over_86}+=$r->{total};
				@times_86=(@times_86,@$uuu);
			}
			if ($upstream eq '192.168.10.254:80'){
				$agg{over_80}+=$r->{total};
			}
			if ($upstream eq '192.168.10.253:80'){
				$agg{over_80_reg}+=$r->{total};
			}
			$agg{'over_'.$code}+=$r->{total};
		}
		my @s_memcache = sort {$a <=> $b} @times_memcache;
		my @s_86 = sort {$a <=> $b} @times_86;
		my @s_82 = sort {$a <=> $b} @times_82;
		my @r_all = sort {$a <=> $b} @resp;
		my @r_html = sort {$a <=> $b} @resp_html;
		my @r_json = sort {$a <=> $b} @resp_json;
		my @r_player = sort {$a <=> $b} @resp_player;
		my @u_player = sort {$a <=> $b} @upstr_player;
		my @r_russia = sort {$a <=> $b} @resp_russia;
		my @u_russia = sort {$a <=> $b} @upstr_russia;
		my @r_russia2 = sort {$a <=> $b} @resp_russia2;
		my @u_russia2 = sort {$a <=> $b} @upstr_russia2;
                my @r_vesti = sort {$a <=> $b} @resp_vesti;
                my @u_vesti = sort {$a <=> $b} @upstr_vesti;
		$agg{memcache_75}=$s_memcache[int(scalar(@s_memcache)*0.75)];
		$agg{apache_86_75}=$s_86[int(scalar(@s_86)*0.75)];
		$agg{resp_90}=$r_all[int(scalar(@r_all)*0.9)];
		$agg{resp_95}=$r_all[int(scalar(@r_all)*0.95)];
		$agg{resp_html_90}=$r_html[int(scalar(@r_html)*0.9)];
		$agg{resp_html_95}=$r_html[int(scalar(@r_html)*0.95)];
		$agg{resp_json_90}=$r_html[int(scalar(@r_json)*0.9)];
		$agg{resp_json_95}=$r_html[int(scalar(@r_json)*0.95)];
		$agg{resp_player_90}=$r_player[int(scalar(@r_player)*0.9)];
		$agg{resp_player_95}=$r_player[int(scalar(@r_player)*0.95)];
		$agg{upstr_player_90}=$u_player[int(scalar(@u_player)*0.9)];
		$agg{upstr_player_95}=$u_player[int(scalar(@u_player)*0.95)];
		$agg{resp_russia_90}=$r_russia[int(scalar(@r_russia)*0.9)];
		$agg{resp_russia_95}=$r_russia[int(scalar(@r_russia)*0.95)];
		$agg{upstr_russia_90}=$u_russia[int(scalar(@u_russia)*0.9)];
		$agg{upstr_russia_95}=$u_russia[int(scalar(@u_russia)*0.95)];
		$agg{resp_russia2_90}=$r_russia2[int(scalar(@r_russia2)*0.9)];
		$agg{resp_russia2_95}=$r_russia2[int(scalar(@r_russia2)*0.95)];
		$agg{upstr_russia2_90}=$u_russia2[int(scalar(@u_russia2)*0.9)];
		$agg{upstr_russia2_95}=$u_russia2[int(scalar(@u_russia2)*0.95)];
		$agg{resp_vesti_90}=$r_vesti[int(scalar(@r_vesti)*0.9)];
                $agg{resp_vesti_95}=$r_vesti[int(scalar(@r_vesti)*0.95)];
                $agg{upstr_vesti_90}=$u_vesti[int(scalar(@u_vesti)*0.9)];
                $agg{upstr_vesti_95}=$u_vesti[int(scalar(@u_vesti)*0.95)];

#		$master->analytics->agg->insert(\%agg);
		open (LL,'>to_zabbix.txt');
		for my $l (keys %agg){
			if ($l eq 'tstamp'){
				 next;
			}
			print LL 'web-farm vgtrk.'.$l.' '.$agg{tstamp}.' '.$agg{$l}."\n"; 
		}
		close LL;
		system('/usr/local/bin/zabbix_sender -z localhost -s web-farm -T -i to_zabbix.txt');
        }

	if ($new_last<time()-80){
		sleep 5;
	}
	$master->analytics->proc->update({last=>$last},{'$set'=>{last=>$new_last}});
	$last=$new_last;	
}

1;
