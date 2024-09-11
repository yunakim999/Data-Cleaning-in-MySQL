-- Data Cleaning

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standarize the Data (issues with spellings..etc)
-- 3. Null Values or Blank Values
-- 4. Remove Any Rows (Duplicate raw data first)

-- 1. Remove Duplicates

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, 'date', stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1
;

SELECT * 
FROM layoffs_staging
WHERE company = 'Apollo'; -- Checking if they have correct duplicates 

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, 'date', stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SET SQL_SAFE_UPDATES = 0; -- Disable safe updates to delete duplicates

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- 2. Standarize the Data (issues with spellings..etc)

-- 2.1 Company
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'; -- Change different industry name under one

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- 2.2 Location
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1; -- No issue

-- 2.3 Country
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1; 

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) -- Removing dot at the end
FROM layoffs_staging2
ORDER BY 1
;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- 2.4 Date
SELECT date
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;

ALTER TABLE layoffs_staging
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;

UPDATE layoffs_staging2 ls2
JOIN layoffs_staging ls ON ls2.id = ls.id
AND ls2.company = ls.company
SET ls2.date = ls.date;

SELECT date
FROM layoffs_staging2;

SELECT DISTINCT date FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE STR_TO_DATE(date, '%m/%d/%Y') IS NULL AND date IS NOT NULL;

DELETE FROM layoffs_staging2
WHERE STR_TO_DATE(date, '%m/%d/%Y') IS NULL AND date IS NOT NULL;

DELETE FROM layoffs_staging2
WHERE date NOT REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$';

UPDATE layoffs_staging2
SET date = str_to_date(date, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN date DATE;

DESCRIBE layoffs_staging2; -- checking datatype

-- 3. Null Values or Blank Values

-- Check companys with null or blank industry
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb'; -- Fill in industry data

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = ''; -- Changing null values to blanks

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 
SET industry = 'travel'
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2;

-- 4. Remove Any Rows not needed for EDA 

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; -- Delete unnecessary rows

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

ALTER TABLE layoffs_staging2
DROP COLUMN id;
