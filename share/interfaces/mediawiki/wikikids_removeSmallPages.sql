DELETE FROM page WHERE page_namespace != 0;
DELETE FROM page WHERE page_len < 3000;
DELETE FROM page WHERE page_is_redirect != 0;
DELETE FROM page WHERE page_title LIKE "Lijst_van%";
