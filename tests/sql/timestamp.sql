create table timestamp_test
(
    ts timestamp
);
insert into timestamp_test
values ('1991-01-08 04:05:06');
insert into timestamp_test
values ('1991-01-09 04:05:06 AD');
insert into timestamp_test
values ('1991-01-10 04:05:06 BC');
insert into timestamp_test
values ('1991-01-14 04:05:06 +08:00');
insert into timestamp_test
values ('1991-01-15 04:05:06 AD +08:00');
insert into timestamp_test
values ('1991-01-16 04:05:06 BC +08:00');
insert into timestamp_test
values ('1991-01-17 04:05:06 +08:00 AD');
insert into timestamp_test
values ('1991-01-18 04:05:06 +08:00 BC');
select *
from timestamp_test
order by ts;
drop table timestamp_test;

create table timestamptz_test
(
    ts timestamptz
);
insert into timestamptz_test
values ('1991-01-08 04:05:06');
insert into timestamptz_test
values ('1991-01-09 04:05:06 AD');
insert into timestamptz_test
values ('1991-01-10 04:05:06 BC');
insert into timestamptz_test
values ('1991-01-14 04:05:06 +08:00');
insert into timestamptz_test
values ('1991-01-15 04:05:06 AD +08:00');
insert into timestamptz_test
values ('1991-01-16 04:05:06 BC +08:00');
insert into timestamptz_test
values ('1991-01-17 04:05:06 +08:00 AD');
insert into timestamptz_test
values ('1991-01-18 04:05:06 +08:00 BC');
select *
from timestamptz_test
order by ts;
drop table timestamptz_test;