SELECT *
FROM PortfolioProject.dbo.CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 3,4

SELECT *
FROM PortfolioProject.dbo.CovidVaccinations


--getting the column datatypes 
USE PortfolioProject;

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'CovidDeaths';



-- Alter the data type of 'total_deaths' and 'new_deaths' column from nvarchar to float

USE PortfolioProject;

ALTER TABLE CovidDeaths
ALTER COLUMN total_deaths FLOAT;

ALTER TABLE CovidDeaths
ALTER COLUMN new_deaths FLOAT;

--date was of the type datetime, so changing it to date only
ALTER TABLE CovidDeaths
ALTER COLUMN date date;




--getting our first dataset
SELECT location, date, population, total_cases, total_deaths
FROM CovidDeaths
WHERE continent IS NOT NULL
ORDER BY location, date


--getting death rate 
--likelihood of dying if contracted
SELECT location, date, total_deaths, total_cases, (total_deaths/total_cases)*100 as deathrate
--instead of changing the datatype of total_deaths and total_cases
--death rate could also be calculated by (CAST(total_deaths as float)/CAST(total_cases as float))*100 
FROM CovidDeaths
WHERE location LIKE '%india%'
and continent IS NOT NULL
ORDER BY 1,2



--Total Case vs Population
--what percent of population got covid
SELECT location, date, total_cases,new_deaths, population, (total_cases/population)*100 as pos_rate
FROM CovidDeaths
WHERE location like '%india%' 
AND continent IS NOT NULL
ORDER BY 2






--Countries with highest infection rate compared to population
SELECT Location, Population, MAX(total_cases) as max_total_case, MAX((total_cases/population)*100) as max_infected_pct
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location,population
ORDER BY max_infected_pct desc






--Countries with the highest death count
SELECT location, population,MAX(total_deaths) as loc_max_death, MAX((total_deaths/population)*100) as fatality_by_pop  
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location, population
ORDER BY MAX(total_deaths) desc





--GROUPING BY CONTINENTS (ordered by highest death count)

SELECT continent, MAX(total_deaths) as totaldeathcount
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY totaldeathcount desc
--there are some discrepancies in the continents column of some countries


--can use this instead as for continents, the location column contains the continent name 
--while the continent column is null :)
SELECT location, MAX(total_deaths) as counttotaldeath
FROM CovidDeaths
WHERE continent is NULL
and location <> 'World'
and location <> 'European Union'
and location <> 'International'
GROUP BY location 
ORDER BY counttotaldeath desc


-- GLOBAL DATA
SELECT * 
FROM PortfolioProject..CovidDeaths
WHERE location = 'World'

--GLOBAL data
--overall total
SELECT SUM(new_cases)as total_cases,SUM(new_deaths) as total_deaths,
(SUM(new_deaths)/SUM(new_cases))*100 as total_death_percent
FROM PortfolioProject.dbo.CovidDeaths
WHERE continent IS NOT NULL
--GLOBAL DEATH PERCENTAGE IS 2.11%

--daily totals
SELECT date, SUM(new_cases) as daily_total_cases, SUM(new_deaths) as daily_total_deaths,
(SUM(new_deaths)/SUM(new_cases))*100 as Daily_death_percent
FROM PortfolioProject..CovidDeaths
WHERE continent is not null
GROUP BY date
ORDER BY 1


--joining both the datesets

SELECT *
FROM PortfolioProject.dbo.CovidDeaths as dea
JOIN PortfolioProject.dbo.CovidVaccinations vac
ON dea.location = vac.location
AND dea.date = vac.date





--daily vaccinations vs population


SELECT dea.continent, dea.date, dea.location, dea.population,new_vaccinations
FROM PortfolioProject..CovidDeaths as dea
JOIN PortfolioProject..CovidVaccinations as vac
ON dea.location = vac.location
WHERE dea.continent is not null
ORDER BY dea.location, dea.date






--checking the daily cases, deaths and vaccinations 
SELECT dea.location, dea.date, dea.population, dea.total_cases, dea.new_cases, dea.new_deaths, dea.total_deaths, vac.new_vaccinations, vac.total_vaccinations
FROM PortfolioProject..CovidDeaths as dea
JOIN PortfolioProject..CovidVaccinations as vac
ON dea.location = vac.location 
and dea.date = vac.date
WHERE dea.continent is not null
ORDER BY 1,2







--total amount of people in the world that have been vaccinated by location
--total population vs total vaccinations
SELECT dea.continent, dea.date, dea.location, dea.population, new_vaccinations 
FROM PortfolioProject..CovidDeaths as dea
JOIN PortfolioProject..CovidVaccinations as vac
	ON dea.location = vac.location
WHERE dea.continent is not null
ORDER BY 2
--GROUP BY dea.continent, dea.location, dea.population, new_vaccinations








--getting the population, total cases, deaths, and vaccinations per location 
SELECT dea.location,AVG(DISTINCT population) as pop, SUM(new_cases) as sumnewcases, SUM(new_deaths)as sumnewdeaths, SUM(CONVERT(float, new_vaccinations)) as sumnewvac
FROM PortfolioProject..CovidDeaths as dea
INNER JOIN PortfolioProject..CovidVaccinations as vac
ON dea.location = vac.location
and dea.date = vac.date
WHERE dea.continent is not null
GROUP BY dea.location 
ORDER BY dea.location







--creating the total columns from scratch using the daily columns
--using the rolling sum

SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(int, vac.new_vaccinations)) OVER (Partition by dea.location 
ORDER BY dea.location, dea.date) as rolling_total_vaccinations
FROM PortfolioProject..CovidDeaths as dea
JOIN PortfolioProject..CovidVaccinations as vac
ON dea.location = vac.location
and dea.date = vac.date
WHERE dea.continent is not null
--and dea.location = 'Gibraltar'
ORDER BY 2,3

--if order by not included in the partition by statement
-- then rolling_total_vaccinations gives just the total and 
--not the rolling sum!!!!!





--CREATING CTE, TEMP TABLE , VIEW of the above dataset

--USE CTE

WITH PopvsVac (Continent, Location, date, population, new_vaccinations, rolling_total_vaccinations)
as
(
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(int, vac.new_vaccinations)) OVER (Partition by dea.location 
ORDER BY dea.location, dea.date) as rolling_total_vaccinations
FROM PortfolioProject..CovidDeaths as dea
JOIN PortfolioProject..CovidVaccinations as vac
ON dea.location = vac.location
and dea.date = vac.date
WHERE dea.continent is not null
--ORDER BY 2,3
--order by is invalid in cte, view
)

SELECT  *, (rolling_total_vaccinations/population)*100 as pct_vaccinated
FROM PopvsVac
WHERE continent is not null





--USING TEMP TABLES

DROP TABLE if exists #tempPopvsVac
CREATE TABLE #tempPopvsVac
(Continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
rolling_total_vaccinations float)

INSERT INTO #tempPopvsVac

SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(int, vac.new_vaccinations)) OVER (Partition by dea.location 
ORDER BY dea.location, dea.date) as rolling_total_vaccinations
FROM PortfolioProject..CovidDeaths as dea
JOIN PortfolioProject..CovidVaccinations as vac
ON dea.location = vac.location
and dea.date = vac.date
WHERE dea.continent is not null

SELECT *, (rolling_total_vaccinations/population)*100 as pct_people_vaccinated
FROM #tempPopvsVac







-- using VIEWS
Create View 
viewPopvsVac AS
SELECT 
dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(int, vac.new_vaccinations)) OVER (Partition by dea.location 
ORDER BY dea.location, dea.date) as rolling_total_vaccinations
FROM PortfolioProject..CovidDeaths as dea
JOIN PortfolioProject..CovidVaccinations as vac
ON dea.location = vac.location
and dea.date = vac.date
WHERE dea.continent is not null
--ORDER BY 2,3 (invalid in a view)

SELECT *
FROM viewPopvsVac