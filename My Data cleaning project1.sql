
create table layoffs_staging
like layoffs;
select * from layoffs_staging;
insert into layoffs_staging
select * from layoffs;
select * from layoffs_staging;













select *, 
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num
from layoffs_staging;

with duplicate_cte as
(
select *, 
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num
from layoffs_staging
)
select * from duplicate_cte
where row_num > 1;

select * from layoffs_staging
where company = 'Cazoo';










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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * from layoffs_staging2;
insert into layoffs_staging2
select *, 
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num
from layoffs_staging;
select * from layoffs_staging2 where row_num > 1;

delete from layoffs_staging2 where row_num > 1;

select * from layoffs_staging2;












CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
`row_num` INT
);






INSERT INTO `world_layoffs`.`layoffs_staging2`
	SELECT *, 
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;
select * from layoffs_staging2;
select * from layoffs_staging2 where row_num > 1;
delete from layoffs_staging2 where row_num > 1;
select * from layoffs_staging2 where row_num > 1;
DELETE FROM `world_layoffs`.`layoffs_staging2`
WHERE row_num > 1;
DELETE FROM `world_layoffs`.`layoffs_staging2`
WHERE row_num > 1;
select * from layoffs_staging2;
select company, trim(company)
from layoffs_staging2;
update layoffs_staging2
set company = trim(company);

select distinct industry from layoffs_staging2 order by 1;
update layoffs_staging2
set industry = 'Crypto' where industry like 'crypto%';

select distinct industry from layoffs_staging2 order by 1;
select distinct country from layoffs_staging2 order by 1;
select * from layoffs_staging2 where country like 'United States%';
update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';
select distinct country from layoffs_staging2 order by 1;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');
select `date` from layoffs_staging2;
alter table layoffs_staging2
modify column `date` date;



select * from layoffs_staging2;
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL or t1.industry = '')
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';

SELECT *
FROM layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry = '' or t1.industry is null or t1.industry = ' ')
and t2.industry is not null;
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL or t1.industry = '' or t1.industry = ' ')
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry = '' or t1.industry is null or t1.industry = ' ')
and t2.industry is not null;

UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry = '' or t1.industry is null or t1.industry = ' ')
and t2.industry is not null;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where t1.industry is null
and t2.industry is not null;

SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

delete
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

alter table layoffs_staging2
drop column row_num;