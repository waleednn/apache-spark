-- mask function
SELECT mask('AbCD123-@$#');
SELECT mask('AbCD123-@$#', 'Q');
SELECT mask('AbCD123-@$#', 'Q', 'q');
SELECT mask('AbCD123-@$#', 'Q', 'q', 'd');
SELECT mask('AbCD123-@$#', 'Q', 'q', 'd', 'o');
SELECT mask('AbCD123-@$#', 'Qa', 'qa', 'da', 'oa');
SELECT mask('AbCD123-@$#', NULL, 'q', 'd', 'o');
SELECT mask('AbCD123-@$#', NULL, NULL, 'd', 'o');
SELECT mask('AbCD123-@$#', NULL, NULL, NULL, 'o');
SELECT mask('AbCD123-@$#', NULL, NULL, NULL, NULL);
SELECT mask(NULL);
SELECT mask(NULL, NULL, 'q', 'd', 'o');
SELECT mask(NULL, NULL, NULL, 'd', 'o');
SELECT mask(NULL, NULL, NULL, NULL, 'o');
SELECT mask('AbCD123-@$#', NULL, NULL, NULL, NULL);
SELECT mask(c1) from values ('AbCD123-@$#') as tab(c1);
SELECT mask(c1, 'Q') from values ('AbCD123-@$#') as tab(c1);
SELECT mask(c1, 'Q', 'q')from values ('AbCD123-@$#') as tab(c1);
SELECT mask(c1, 'Q', 'q', 'd') from values ('AbCD123-@$#') as tab(c1);
SELECT mask(c1, 'Q', 'q', 'd', 'o') from values ('AbCD123-@$#') as tab(c1);
SELECT mask(c1, NULL, 'q', 'd', 'o') from values ('AbCD123-@$#') as tab(c1);
SELECT mask(c1, NULL, NULL, 'd', 'o') from values ('AbCD123-@$#') as tab(c1);
SELECT mask(c1, NULL, NULL, NULL, 'o') from values ('AbCD123-@$#') as tab(c1);
SELECT mask(c1, NULL, NULL, NULL, NULL) from values ('AbCD123-@$#') as tab(c1);
SELECT mask(c1, NULL, 'q', 'd', 'o') from values ('AbCD123-@$#') as tab(c1);
SELECT mask(c1, 'Q', NULL, 'd', 'o') from values ('AbCD123-@$#') as tab(c1);
SELECT mask(c1, 'Q', 'q', NULL, 'o') from values ('AbCD123-@$#') as tab(c1);
SELECT mask(c1, 'Q', 'q', 'd', NULL) from values ('AbCD123-@$#') as tab(c1);
SELECT mask(NULL, 'Q', 'q', 'd', NULL) from values ('AbCD123-@$#') as tab(c1);
SELECT mask('abcd-EFGH-8765-4321');
SELECT mask('abcd-EFGH-8765-4321', 'Q');
SELECT mask('abcd-EFGH-8765-4321', 'Q', 'q');
SELECT mask('abcd-EFGH-8765-4321', 'Q', 'q', 'd');
SELECT mask('abcd-EFGH-8765-4321', 'Q', 'q', 'd', '*');
SELECT mask('abcd-EFGH-8765-4321', NULL, 'q', 'd', '*');
SELECT mask('abcd-EFGH-8765-4321', NULL, NULL, 'd', '*');
SELECT mask('abcd-EFGH-8765-4321', NULL, NULL, NULL, '*');
SELECT mask('abcd-EFGH-8765-4321', NULL, NULL, NULL, NULL);
SELECT mask(NULL);
SELECT mask(NULL, NULL, 'q', 'd', '*');
SELECT mask(NULL, NULL, NULL, 'd', '*');
SELECT mask(NULL, NULL, NULL, NULL, '*');
SELECT mask(NULL, NULL, NULL, NULL, NULL);
SELECT mask(c1) from values ('abcd-EFGH-8765-4321') as tab(c1);
SELECT mask(c1, 'Q') from values ('abcd-EFGH-8765-4321') as tab(c1);
SELECT mask(c1, 'Q', 'q')from values ('abcd-EFGH-8765-4321') as tab(c1);
SELECT mask(c1, 'Q', 'q', 'd') from values ('abcd-EFGH-8765-4321') as tab(c1);
SELECT mask(c1, 'Q', 'q', 'd', '*') from values ('abcd-EFGH-8765-4321') as tab(c1);
SELECT mask(c1, NULL, 'q', 'd', '*') from values ('abcd-EFGH-8765-4321') as tab(c1);
SELECT mask(c1, NULL, NULL, 'd', '*') from values ('abcd-EFGH-8765-4321') as tab(c1);
SELECT mask(c1, NULL, NULL, NULL, '*') from values ('abcd-EFGH-8765-4321') as tab(c1);
SELECT mask(c1, NULL, NULL, NULL, NULL) from values ('abcd-EFGH-8765-4321') as tab(c1);
SELECT mask(c1, replaceArg) from values('abcd-EFGH-8765-4321', 'a') as t(c1, replaceArg);
SELECT mask(c1, replaceArg) from values('abcd-EFGH-8765-4321', 'ABC') as t(c1, replaceArg);
SELECT mask(c1, replaceArg) from values('abcd-EFGH-8765-4321', 123) as t(c1, replaceArg);
SELECT mask('abcd-EFGH-8765-4321', 'A', 'w', '');
SELECT mask('abcd-EFGH-8765-4321', 'A', 'abc');