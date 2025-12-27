CREATE TABLE files AS

SELECT	mother_folder,
		day,
		file_count
FROM user_files

UNION ALL 

SELECT	mother_folder,
		day,
		file_count
FROM user_files_2025

UNION ALL 

SELECT	mother_folder,
		day,
		file_count
FROM user_files_alumni

ORDER BY day ASC

