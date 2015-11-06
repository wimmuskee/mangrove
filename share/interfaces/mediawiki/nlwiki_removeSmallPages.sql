ALTER TABLE page ADD INDEX index_page_namespace (page_namespace);
DELETE FROM page WHERE page_namespace != 0;
-- incremental deletes, too large delete for default innodb ibdata size
DELETE FROM page WHERE page_len < 50;
DELETE FROM page WHERE page_len < 500;
DELETE FROM page WHERE page_len < 750;
DELETE FROM page WHERE page_len < 1000;
DELETE FROM page WHERE page_len < 1500;
DELETE FROM page WHERE page_len < 3000;
DELETE FROM page WHERE page_is_redirect != 0;
DELETE FROM page WHERE page_title LIKE "Lijst_van%";
