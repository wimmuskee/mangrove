DROP TABLE IF EXISTS nlwiki_page;
RENAME TABLE page TO nlwiki_page;
DROP TABLE IF EXISTS nlwiki_categories;
RENAME TABLE categories TO nlwiki_categories;
DROP TABLE IF EXISTS nlwiki_categorylinks;
RENAME TABLE categorylinks TO nlwiki_categorylinks;
DROP TABLE IF EXISTS nlwiki_category_relations;
RENAME TABLE category_relations TO nlwiki_category_relations;
