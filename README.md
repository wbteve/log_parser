log_parser
==========

nginx logs parser and stats counter

nginx log format
================

        log_format  main '$remote_addr    $remote_user    $time_local    $request_method    $scheme://$host$request_uri    $server_protocol    '
                          '$status    $body_bytes_sent    $http_referer    '
                          '$http_user_agent    $request_time    $upstream_response_time    $upstream_addr    $upstream_cache_status    $upstream_status    $memcached_key    $enhanced_memcached_key    '
                          '$sent_http_x_backend    $request_completion    $sent_http_content_type    $request_id    $uid_got    $uid_set';
