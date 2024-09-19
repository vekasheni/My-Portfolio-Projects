------------- DATA CLEANING --------------

SELECT * FROM layoffs;

-- First thing we want to do is create a staging table. 
-- This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT layoffs_staging 
SELECT * FROM layoffs;

-- Now when we are data cleaning we usually follow a few steps
-- 1. Check for duplicates and remove any
-- 2. Standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. Remove any columns and rows that are not necessary - few ways


-- 1. Remove Duplicates

# First let's check for duplicates

SELECT * FROM layoffs_staging;

SELECT * ,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(SELECT * ,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging)
SELECT * FROM duplicate_cte
WHERE row_num > 1;
    
-- Let's just look at Oda to confirm
SELECT *
FROM layoffs_staging
WHERE company = 'Oda';
-- It looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- These are our real duplicates 
SELECT * FROM 
(SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging) duplicate_cte
WHERE row_num > 1;

-- These are the ones we want to delete where the row number is > 1 or 2 or greater essentially

-- Now you may want to write it like this:
WITH DELETE_CTE AS 
(SELECT * ,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging) 
DELETE FROM delete_cte
WHERE row_num > 1;

-- One solution, which I think is a good one. Is to create a new column and add those row numbers in. 
-- Then delete where row numbers are over 2, then delete that column
-- so let's do it!!

CREATE TABLE `layoffs_staging2` 
(`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT);


SELECT * FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO `layoffs_staging2`
SELECT * ,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Now that we have this we can delete rows were row_num is greater than 1

DELETE FROM layoffs_staging2
WHERE row_num > 1;

SELECT * FROM layoffs_staging2;


-- 2. Standardize Data

SELECT * FROM layoffs_staging2;

-- Company column has extra spaces. We can trim those.
SELECT company,TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- If we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

-- Changing industry from CryptoCurrency to Crypto
SELECT * FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Let's check the same for country column
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- Now removing the extra dot at the end of the name
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Now if we look at date column, it is in text format. We need to change it to datetime format
SELECT `date`,
STR_TO_DATE(`date`,'%m %d %Y')
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Checking for null values in both total_laid_off and percentage_laid_off columns
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- Industry column has both missing as well as null values
SELECT * FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

-- Let's take a look at these
SELECT * FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

-- It looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. 
-- What we can do is write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- Makes it easy so if there were thousands we wouldn't have to manually check them all

-- We should set the blanks to nulls since those are typically easier to work with
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Now if we check those are all null
SELECT * FROM layoffs_staging2 WHERE industry IS NULL OR industry = ''
ORDER BY industry;

-- Now we need to populate those nulls if possible
SELECT * FROM layoffs_staging2 t1 JOIN layoffs_staging2 t2 
ON t1.company = t2.company
AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1 JOIN layoffs_staging2 t2 
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- And if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT * FROM layoffs_staging2 WHERE industry IS NULL OR industry = ''
ORDER BY industry;

SELECT * FROM layoffs_staging2;



-- 3. Look at Null Values

-- The null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- So there isn't anything I want to change with the null values


-- 4. Remove any columns and rows we need to

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * FROM layoffs_staging2;