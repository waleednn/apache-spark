-- test cases for collation support

-- Create a test table with data
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values('aaa', 'aaa');
insert into t1 values('AAA', 'AAA');
insert into t1 values('bbb', 'bbb');
insert into t1 values('BBB', 'BBB');

-- describe
describe table t1;

-- group by and count utf8_binary
select count(*) from t1 group by utf8_binary;

-- group by and count utf8_lcase
select count(*) from t1 group by utf8_lcase;

-- filter equal utf8_binary
select * from t1 where utf8_binary = 'aaa';

-- filter equal utf8_lcase
select * from t1 where utf8_lcase = 'aaa' collate utf8_lcase;

-- filter less then utf8_binary
select * from t1 where utf8_binary < 'bbb';

-- filter less then utf8_lcase
select * from t1 where utf8_lcase < 'bbb' collate utf8_lcase;

-- inner join
select l.utf8_binary, r.utf8_lcase from t1 l join t1 r on l.utf8_lcase = r.utf8_lcase;

-- create second table for anti-join
create table t2(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t2 values('aaa', 'aaa');
insert into t2 values('bbb', 'bbb');

-- anti-join on lcase
select * from t1 anti join t2 on t1.utf8_lcase = t2.utf8_lcase;

drop table t2;
drop table t1;

-- set operations
select col1 collate utf8_lcase from values ('aaa'), ('AAA'), ('bbb'), ('BBB'), ('zzz'), ('ZZZ') except select col1 collate utf8_lcase from values ('aaa'), ('bbb');
select col1 collate utf8_lcase from values ('aaa'), ('AAA'), ('bbb'), ('BBB'), ('zzz'), ('ZZZ') except all select col1 collate utf8_lcase from values ('aaa'), ('bbb');
select col1 collate utf8_lcase from values ('aaa'), ('AAA'), ('bbb'), ('BBB'), ('zzz'), ('ZZZ') union select col1 collate utf8_lcase from values ('aaa'), ('bbb');
select col1 collate utf8_lcase from values ('aaa'), ('AAA'), ('bbb'), ('BBB'), ('zzz'), ('ZZZ') union all select col1 collate utf8_lcase from values ('aaa'), ('bbb');
select col1 collate utf8_lcase from values ('aaa'), ('bbb'), ('BBB'), ('zzz'), ('ZZZ') intersect select col1 collate utf8_lcase from values ('aaa'), ('bbb');

-- create table with struct field
create table t1 (c1 struct<utf8_binary: string collate utf8_binary, utf8_lcase: string collate utf8_lcase>) USING PARQUET;

insert into t1 values (named_struct('utf8_binary', 'aaa', 'utf8_lcase', 'aaa'));
insert into t1 values (named_struct('utf8_binary', 'AAA', 'utf8_lcase', 'AAA'));

-- aggregate against nested field utf8_binary
select count(*) from t1 group by c1.utf8_binary;

-- aggregate against nested field utf8_lcase
select count(*) from t1 group by c1.utf8_lcase;

drop table t1;

-- array function tests
select array_contains(ARRAY('aaa' collate utf8_lcase),'AAA' collate utf8_lcase);
select array_position(ARRAY('aaa' collate utf8_lcase, 'bbb' collate utf8_lcase),'BBB' collate utf8_lcase);

-- utility
select nullif('aaa' COLLATE utf8_lcase, 'AAA' COLLATE utf8_lcase);
select least('aaa' COLLATE utf8_lcase, 'AAA' collate utf8_lcase, 'a' collate utf8_lcase);

-- array operations
select arrays_overlap(array('aaa' collate utf8_lcase), array('AAA' collate utf8_lcase));
select array_distinct(array('aaa' collate utf8_lcase, 'AAA' collate utf8_lcase));
select array_union(array('aaa' collate utf8_lcase), array('AAA' collate utf8_lcase));
select array_intersect(array('aaa' collate utf8_lcase), array('AAA' collate utf8_lcase));
select array_except(array('aaa' collate utf8_lcase), array('AAA' collate utf8_lcase));

-- ICU collations (all statements return true)
select 'a' collate unicode < 'A';
select 'a' collate unicode_ci = 'A';
select 'a' collate unicode_ai = 'å';
select 'a' collate unicode_ci_ai = 'Å';
select 'a' collate en < 'A';
select 'a' collate en_ci = 'A';
select 'a' collate en_ai = 'å';
select 'a' collate en_ci_ai = 'Å';
select 'Kypper' collate sv < 'Köpfe';
select 'Kypper' collate de > 'Köpfe';
select 'I' collate tr_ci = 'ı';

-- create table for str_to_map
create table t4 (text string collate utf8_binary, pairDelim string collate utf8_lcase, keyValueDelim string collate utf8_binary) using parquet;

insert into t4 values('a:1,b:2,c:3', ',', ':');

select str_to_map(text, pairDelim, keyValueDelim) from t4;
select str_to_map(text collate utf8_binary, pairDelim collate utf8_lcase, keyValueDelim collate utf8_binary) from t4;
select str_to_map(text collate utf8_binary, pairDelim collate utf8_binary, keyValueDelim collate utf8_binary) from t4;

drop table t4;

-- ConcatWs
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('Spark', 'SQL');
select concat_ws(' ', utf8_binary, utf8_lcase) from t1;
select concat_ws(' ' collate utf8_binary, utf8_binary, 'SQL' collate utf8_lcase) from t1;
select concat_ws(',', utf8_lcase, 'word'), concat_ws(',', utf8_binary, 'word') from t1;
select concat_ws(',', utf8_lcase, 'word' collate utf8_binary), concat_ws(',', utf8_binary, 'word' collate utf8_lcase) from t1;
drop table t1;

-- Elt
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('Spark', 'SQL');
select elt(2, utf8_binary, utf8_lcase) from t1;
select elt(1, utf8_binary collate utf8_lcase, utf8_lcase) from t1;
select elt(1, utf8_binary, 'word'), elt(1, utf8_lcase, 'word') from t1;
drop table t1;

-- SplitPart
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('aaAaAAaA', 'aaAaAAaA');
select split_part(utf8_binary, utf8_lcase, 3) from t1;
select split_part(utf8_binary, 'a', 3), split_part(utf8_lcase, 'a', 3) from t1;
select split_part(utf8_binary, 'a' collate utf8_lcase, 3), split_part(utf8_lcase, 'a' collate utf8_binary, 3) from t1;
drop table t1;

-- Contains
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('aaAaAAaA', 'aaAaaAaA');
select contains(utf8_binary, utf8_lcase) from t1;
select contains(utf8_binary collate utf8_lcase, utf8_lcase), contains(utf8_binary, utf8_lcase collate utf8_binary) from t1;
select contains(utf8_binary, 'AAa'), contains(utf8_lcase, 'AaAA') from t1;
drop table t1;

-- SubstringIndex
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('aaAaAAaA', 'aaAaaAaA');
insert into t1 values ('aaAaAAaA', 'aaAaaAaAaaAaaAaAaaAaaAaA');
select substring_index(utf8_binary, utf8_lcase, 1) from t1;
select substring_index(utf8_lcase, utf8_binary collate utf8_lcase, 3), substring_index(utf8_lcase collate utf8_binary, utf8_binary, -3) from t1;
select substring_index(utf8_binary, 'AAa', 1), substring_index(utf8_lcase, 'AaAA', 3) from t1;
drop table t1;

-- StringInStr
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('aaAaAAaA', 'aaAaaAaA');
insert into t1 values ('aaAaAAaA', 'aaAaaAaAaaAaaAaAaaAaaAaA');
select instr(utf8_binary, utf8_lcase) from t1;
select instr(utf8_lcase, utf8_binary collate utf8_lcase), instr(utf8_lcase collate utf8_binary, utf8_binary) from t1;
select instr(utf8_binary, 'AAa'), instr(utf8_lcase, 'AaAA') from t1;
drop table t1;

-- FindInSet
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('aaAaAAaA', 'aaAaaAaA');
insert into t1 values ('İo', 'İo');
select find_in_set(utf8_binary, utf8_lcase) from t1;
select find_in_set(utf8_binary, 'aaAaaAaA,i̇o'), find_in_set(utf8_lcase, 'aaAaaAaA,i̇o') from t1;
select find_in_set(utf8_binary, 'aaAaaAaA,i̇o' collate utf8_lcase), find_in_set(utf8_lcase, 'aaAaaAaA,i̇o' collate utf8_binary) from t1;
drop table t1;

-- StartsWith
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('aaAaAAaA', 'aaAaaAaA');
insert into t1 values ('İo', 'İo');
select startswith(utf8_binary, utf8_lcase) from t1;
select startswith(utf8_binary, 'aaAaaAaA'), startswith(utf8_lcase, 'aaAaaAaA') from t1;
select startswith(utf8_binary, 'aaAaaAaA' collate utf8_lcase), startswith(utf8_lcase, 'aaAaaAaA' collate utf8_binary) from t1;
drop table t1;

-- StringTranslate
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('aaAaAAaA', 'aaAaaAaA');
insert into t1 values ('İo', 'İo');
select translate(utf8_binary, utf8_lcase collate utf8_lcase, 'abc' collate utf8_binary) from t1;
select translate(utf8_binary, 'aaAaaAaA', '12345'), translate(utf8_lcase, 'aaAaaAaA', '12345') from t1;
select translate(utf8_binary, 'aaAaaAaA' collate utf8_lcase, '12345'), translate(utf8_lcase, 'aaAaaAaA' collate utf8_binary, '12345') from t1;
drop table t1;

-- Replace
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('aaAaAAaA', 'aaAaaAaA');
insert into t1 values ('İo', 'İo');
select replace(utf8_binary, utf8_lcase collate utf8_lcase, 'abc' collate utf8_binary) from t1;
select replace(utf8_binary, 'aaAaaAaA', '12345'), replace(utf8_lcase, 'aaAaaAaA', '12345') from t1;
select replace(utf8_binary, 'aaAaaAaA' collate utf8_lcase, '12345'), replace(utf8_lcase, 'aaAaaAaA' collate utf8_binary, '12345') from t1;
drop table t1;

-- EndsWith
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('aaAaAAaA', 'aaAaaAaA');
insert into t1 values ('İo', 'i̇o');
select endswith(utf8_binary, utf8_lcase) from t1;
select endswith(utf8_binary, 'aaAaaAaA'), endswith(utf8_lcase, 'aaAaaAaA') from t1;
select endswith(utf8_binary, utf8_lcase collate utf8_binary), endswith(utf8_lcase, utf8_binary collate utf8_lcase) from t1;
drop table t1;

-- StringRepeat
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('aaAaAAaA', 'aaAaaAaA');
insert into t1 values ('İo', 'i̇o');
select repeat(utf8_binary, 3), repeat(utf8_lcase, 2) from t1;
select repeat(utf8_binary collate utf8_lcase, 3), repeat(utf8_lcase collate utf8_binary, 2) from t1;
drop table t1;

-- Ascii & UnBase64 string expressions
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('aaAaAAaA', 'aaAaaAaA');
insert into t1 values ('efd2', 'efd2');
select ascii(utf8_binary), ascii(utf8_lcase) from t1;
select ascii(utf8_binary collate utf8_lcase), ascii(utf8_lcase collate utf8_binary) from t1;
select unbase64(utf8_binary), unbase64(utf8_lcase) from t1;
select unbase64(utf8_binary collate utf8_lcase), unbase64(utf8_lcase collate utf8_binary) from t1;
drop table t1;

-- Chr
create table t1(ascii long) using parquet;
insert into t1 values (97);
insert into t1 values (66);
select chr(ascii) from t1;
drop table t1;

-- Base64, Decode
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('efd2', 'efd2');
select base64(utf8_binary), base64(utf8_lcase) from t1;
select base64(utf8_binary collate utf8_lcase), base64(utf8_lcase collate utf8_binary) from t1;
select decode(encode(utf8_binary, 'utf-8'), 'utf-8'), decode(encode(utf8_lcase, 'utf-8'), 'utf-8') from t1;
select decode(encode(utf8_binary collate utf8_lcase, 'utf-8'), 'utf-8'), decode(encode(utf8_lcase collate utf8_binary, 'utf-8'), 'utf-8') from t1;
drop table t1;

-- FormatNumber
create table t1(ascii double) using parquet;
insert into t1 values (97.52143);
insert into t1 values (66.421);
select format_number(ascii, '###.###') from t1;
drop table t1;

-- Encode, ToBinary
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('efd2', 'efd2');
select encode(utf8_binary, 'utf-8'), encode(utf8_lcase, 'utf-8') from t1;
select encode(utf8_binary collate utf8_lcase, 'utf-8'), encode(utf8_lcase collate utf8_binary, 'utf-8') from t1;
select to_binary(utf8_binary, 'utf-8'), to_binary(utf8_lcase, 'utf-8') from t1;
select to_binary(utf8_binary collate utf8_lcase, 'utf-8'), to_binary(utf8_lcase collate utf8_binary, 'utf-8') from t1;
drop table t1;

-- Sentences
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('Hello, world! Nice day.', 'Hello, world! Nice day.');
insert into t1 values ('Something else. Nothing here.', 'Something else. Nothing here.');
select sentences(utf8_binary), sentences(utf8_lcase) from t1;
select sentences(utf8_binary collate utf8_lcase), sentences(utf8_lcase collate utf8_binary) from t1;
drop table t1;

-- Upper
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('Hello, world! Nice day.', 'Hello, world! Nice day.');
insert into t1 values ('Something else. Nothing here.', 'Something else. Nothing here.');
select upper(utf8_binary), upper(utf8_lcase) from t1;
select upper(utf8_binary collate utf8_lcase), upper(utf8_lcase collate utf8_binary) from t1;
drop table t1;

-- Lower
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('Hello, world! Nice day.', 'Hello, world! Nice day.');
insert into t1 values ('Something else. Nothing here.', 'Something else. Nothing here.');
select lower(utf8_binary), lower(utf8_lcase) from t1;
select lower(utf8_binary collate utf8_lcase), lower(utf8_lcase collate utf8_binary) from t1;
drop table t1;

-- InitCap
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('Hello, world! Nice day.', 'Hello, world! Nice day.');
insert into t1 values ('Something else. Nothing here.', 'Something else. Nothing here.');
select initcap(utf8_binary), initcap(utf8_lcase) from t1;
select initcap(utf8_binary collate utf8_lcase), initcap(utf8_lcase collate utf8_binary) from t1;
drop table t1;

-- Overlay
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('aaAaAAaA', 'aaAaaAaA');
insert into t1 values ('aaAaAAaA', 'aaAaaAaAaaAaaAaAaaAaaAaA');
select overlay(utf8_binary, utf8_lcase, 3) from t1;
select overlay(utf8_lcase, utf8_binary collate utf8_lcase, 3), overlay(utf8_lcase collate utf8_binary, utf8_binary, 3) from t1;
select overlay(utf8_binary, 'AAa', 3), overlay(utf8_lcase, 'AaAA', 3) from t1;
drop table t1;

-- FormatString
create table t1(format string collate utf8_binary, utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('%s%s', 'abCdE', 'abCdE');
select format_string(format, utf8_binary, utf8_lcase) from t1;
select format_string(format collate utf8_lcase, utf8_lcase, utf8_binary collate utf8_lcase, 3), format_string(format, utf8_lcase collate utf8_binary, utf8_binary) from t1;
select format_string(format, utf8_binary, utf8_lcase) from t1;
drop table t1;

-- SoundEx
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('efd2', 'efd2');
select soundex(utf8_binary), soundex(utf8_lcase) from t1;
select soundex(utf8_binary collate utf8_lcase), soundex(utf8_lcase collate utf8_binary) from t1;
drop table t1;

-- Length, BitLength & OctetLength
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('efd2', 'efd2');
select length(utf8_binary), length(utf8_lcase) from t1;
select length(utf8_binary collate utf8_lcase), length(utf8_lcase collate utf8_binary) from t1;
select bit_length(utf8_binary), bit_length(utf8_lcase) from t1;
select bit_length(utf8_binary collate utf8_lcase), bit_length(utf8_lcase collate utf8_binary) from t1;
select octet_length(utf8_binary), octet_length(utf8_lcase) from t1;
select octet_length(utf8_binary collate utf8_lcase), octet_length(utf8_lcase collate utf8_binary) from t1;
drop table t1;

-- Luhncheck
create table t1(num long) using parquet;
insert into t1 values (97);
insert into t1 values (66);
select luhn_check(num) from t1;
drop table t1;

-- Levenshtein
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('kitten', 'sitTing');
select levenshtein(utf8_binary, utf8_lcase) from t1;
select levenshtein(utf8_binary, 'aaAaaAaA'), levenshtein(utf8_lcase, 'aaAaaAaA') from t1;
select levenshtein(utf8_binary, utf8_lcase collate utf8_binary), levenshtein(utf8_lcase, utf8_binary collate utf8_lcase) from t1;
drop table t1;

-- IsValidUTF8
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('efd2', 'efd2');
insert into t1 values ('abc', 'abc');
select is_valid_utf8(utf8_binary), is_valid_utf8(utf8_lcase) from t1;
select is_valid_utf8(utf8_binary collate utf8_lcase), is_valid_utf8(utf8_lcase collate utf8_binary) from t1;
drop table t1;

-- MakeValidUTF8
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('efd2', 'efd2');
insert into t1 values ('abc', 'abc');
select make_valid_utf8(utf8_binary), make_valid_utf8(utf8_lcase) from t1;
select make_valid_utf8(utf8_binary collate utf8_lcase), make_valid_utf8(utf8_lcase collate utf8_binary) from t1;
drop table t1;

-- ValidateUTF8
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('efd2', 'efd2');
insert into t1 values ('abc', 'abc');
select validate_utf8(utf8_binary), validate_utf8(utf8_lcase) from t1;
select validate_utf8(utf8_binary collate utf8_lcase), validate_utf8(utf8_lcase collate utf8_binary) from t1;
drop table t1;

-- TryValidateUTF8
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('efd2', 'efd2');
insert into t1 values ('abc', 'abc');
select try_validate_utf8(utf8_binary), try_validate_utf8(utf8_lcase) from t1;
select try_validate_utf8(utf8_binary collate utf8_lcase), try_validate_utf8(utf8_lcase collate utf8_binary) from t1;
drop table t1;

-- Left/Right/Substr
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('efd2', 'efd2');
insert into t1 values ('abc', 'abc');
select substr(utf8_binary, 2, 2), substr(utf8_lcase, 2, 2) from t1;
select substr(utf8_binary collate utf8_lcase, 2, 2), substr(utf8_lcase collate utf8_binary, 2, 2) from t1;
select right(utf8_binary, 2), right(utf8_lcase, 2) from t1;
select right(utf8_binary collate utf8_lcase, 2), right(utf8_lcase collate utf8_binary, 2) from t1;
select left(utf8_binary, '2' collate utf8_lcase), left(utf8_lcase, 2) from t1;
select left(utf8_binary collate utf8_lcase, 2), left(utf8_lcase collate utf8_binary, 2) from t1;
drop table t1;

-- StringRPad
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('efd2', 'efd2');
insert into t1 values ('abc', 'abc');
select rpad(utf8_binary, 8, utf8_lcase) from t1;
select rpad(utf8_lcase, 8, utf8_binary collate utf8_lcase), rpad(utf8_lcase collate utf8_binary, 8, utf8_binary) from t1;
select rpad(utf8_binary, 8, 'AAa'), rpad(utf8_lcase, 8, 'AaAA') from t1;
drop table t1;

-- StringLPad
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('efd2', 'efd2');
insert into t1 values ('abc', 'abc');
select lpad(utf8_binary, 8, utf8_lcase) from t1;
select lpad(utf8_lcase, 8, utf8_binary collate utf8_lcase), lpad(utf8_lcase collate utf8_binary, 8, utf8_binary) from t1;
select lpad(utf8_binary, 8, 'AAa'), lpad(utf8_lcase, 8, 'AaAA') from t1;
drop table t1;

-- Locate
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('efd2', 'efD2');
insert into t1 values ('abc', 'aBc');
select locate(utf8_binary, utf8_lcase) from t1;
select locate(utf8_lcase, utf8_binary collate utf8_lcase), locate(utf8_lcase collate utf8_binary, utf8_binary) from t1;
select locate('B', utf8_binary), locate('B', utf8_lcase) from t1;
drop table t1;

-- StringTrim*
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('abcdcba', 'aBcDCbA');
select TRIM(utf8_binary, utf8_lcase) from t1;
select TRIM(utf8_lcase, utf8_binary collate utf8_lcase), TRIM(utf8_lcase collate utf8_binary, utf8_binary) from t1;
select TRIM('ABC', utf8_binary), TRIM('ABC', utf8_lcase) from t1;
select BTRIM(utf8_binary, utf8_lcase) from t1;
select BTRIM(utf8_lcase, utf8_binary collate utf8_lcase), BTRIM(utf8_lcase collate utf8_binary, utf8_binary) from t1;
select BTRIM('ABC', utf8_binary), BTRIM('ABC', utf8_lcase) from t1;
select LTRIM(utf8_binary, utf8_lcase) from t1;
select LTRIM(utf8_lcase, utf8_binary collate utf8_lcase), LTRIM(utf8_lcase collate utf8_binary, utf8_binary) from t1;
select LTRIM('ABC', utf8_binary), LTRIM('ABC', utf8_lcase) from t1;
select RTRIM(utf8_binary, utf8_lcase) from t1;
select RTRIM(utf8_lcase, utf8_binary collate utf8_lcase), RTRIM(utf8_lcase collate utf8_binary, utf8_binary) from t1;
select RTRIM('ABC', utf8_binary), RTRIM('ABC', utf8_lcase) from t1;
drop table t1;