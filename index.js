const puppeteer = require('puppeteer');
const { Pool } = require('pg');

// PostgreSQL connection
const pool = new Pool({
    user: 'damu',
    host: 'localhost',
    database: 'postgres',
    password: 'damu007',
    port: 5432,
});

async function insertCarIntoDB(car) {
    const { id, brand, model, year, volume, price, cartype } = car;
    try {
        await pool.query(
            `INSERT INTO car (car_id, brand, model, year, volume, price, cartype) 
             VALUES ($1, $2, $3, $4, $5, $6, $7) 
             ON CONFLICT (car_id) DO NOTHING`,
            [id, brand, model, year, volume, price, cartype]
        );
        console.log(`Inserted: ${brand} ${model} (${year})`);
    } catch (error) {
        console.error(`Error inserting ${brand} ${model} (${year}):`, error.message);

        // Log the error into `carinsert_error_log`
        await pool.query(
            `INSERT INTO carinsert_error_log (car_id, brand, model, year, volume, price, cartype, error_message) 
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
            [id, brand, model, year, volume, price, cartype, error.message]
        );
    }
}

async function scrapeDynamicWebsite(url, cartype) {
    const browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();
    await page.goto(url, { waitUntil: 'domcontentloaded' });

    const cars = await page.evaluate(() => {
        function createCarObject(card) {
            const titleElement = card.querySelector(".a-card__title");
            const title = titleElement ? titleElement.innerText.trim() : "";
            const [brand, ...modelParts] = title.split(" ");
            const model = modelParts.join(" ");

            const imgElement = card.querySelector(".a-card__picture img");
            const yearMatch = imgElement?.alt?.match(/\d{4}/);
            const year = yearMatch ? parseInt(yearMatch[0]) : null;

            const descElement = card.querySelector(".a-card__description");
            const desc = descElement ? descElement.innerText : "";

            // Extract volume (handles both "2 л" and "2.0 л")
            const volumeMatch = desc.match(/(\d+(?:\.\d+)?) л/);
            const volume = volumeMatch ? parseFloat(volumeMatch[1]) : null;

            const priceElement = card.querySelector(".a-card__price");
            const priceText = priceElement ? priceElement.innerText.replace(/[^\d]/g, "") : "";
            const price = priceText ? parseInt(priceText) : null;

            // Extract car ID
            const id = card.dataset.id || (card.querySelector("a")?.href.match(/(\d+)$/)?.[1] || null);

            return { id, brand, model, year, volume, price };
        }

        return Array.from(document.querySelectorAll(".a-card")).map(createCarObject);
    });

    await browser.close();

    // Insert data into PostgreSQL
    for (const car of cars) {
        if (car.id) {
            await insertCarIntoDB({ ...car, cartype });
        }
    }
}
async function startScraping() {

    const carType = 'Легковые'.toLowerCase();
    // const carBrands = ['audi'];
    const carBrands = ['audi', 'bmw', 'mercedes-benz', 'volkswagen', 'volvo', 'toyota', 'nissan', 'honda', 'mazda', 'mitsubishi', 'subaru', 'suzuki', 'kia', 'hyundai'];

    pageLimit = 80;
    for (let i = 2; i <= pageLimit; i++) {
        for (const brand of carBrands) {
            let url = `https://kolesa.kz/cars/${brand}`;
            if (i > 1) {
                url += `?page=${i}`;
            }
            await scrapeDynamicWebsite(url, carType);
        }
    }
}

startScraping();


