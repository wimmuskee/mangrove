DROP TABLE IF EXISTS wikikids_page;
RENAME TABLE page TO wikikids_page;
DROP TABLE IF EXISTS wikikids_categories;
RENAME TABLE categories TO wikikids_categories;
DROP TABLE IF EXISTS wikikids_categorylinks;
RENAME TABLE categorylinks TO wikikids_categorylinks;
DROP TABLE IF EXISTS wikikids_category_relations;
RENAME TABLE category_relations TO wikikids_category_relations;
