select * from layoffs_staging2;

select * from layoffs_staging2 where percentage_laid_off = 1
order by funds_raised_millions desc;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select company, total_laid_off from layoffs_staging2 order by company;

select company, sum(total_laid_off) from layoffs_staging2 
group by company order by 2 desc;

select min(`date`), max(`date`) from layoffs_staging2;



select country, sum(total_laid_off) from layoffs_staging2 
group by country order by 2 desc;

select year(`date`), sum(total_laid_off) from layoffs_staging2 
group by year(`date`) order by 2 desc;

select month(`date`), sum(total_laid_off) from layoffs_staging2 
group by month(`date`);

select substring(`date`,1,7) as `month`, sum(total_laid_off) from layoffs_staging2 
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;

with rolling_total as
(
select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off from layoffs_staging2 
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`, total_off, sum(total_off)
over (order by `month`) as roll_total 
from rolling_total;

select company, sum(total_laid_off) from layoffs_staging2 
group by company order by 2 desc;

-- my own code
select company, year(`date`), sum(total_laid_off)
over (partition by company, year(`date`) order by year(`date`)) as layoff_by_year
from layoffs_staging2;

-- from youtube video
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by company asc;

-- code to get year wise info 
with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
)
select *,
dense_rank() over (partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null;


-- code to get year wise info but based on rank like in the first year the greatest layoff, then the greatest layoff the the next year, so on
with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
)
select *,
dense_rank() over (partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
order by ranking;


-- to find the top 5 companies for each year with the largest layoffs
with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
),
company_year_rank as
(select *,
dense_rank() over (partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
)
select * from company_year_rank
where ranking <= 5;




-- testing
with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
)
select *,
dense_rank() over (partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null;