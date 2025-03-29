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

async function insertListingIntoDB(listing) {
    const { listing_id, date, city, address, price, bedrooms, bathrooms, sqr_meters, floor, yr_built, seller_type, ceiling_height } = listing;
    try {
        await pool.query(
            `INSERT INTO listings (listing_id, date,city, address, price, bedrooms, bathrooms, sqr_meters, floor, yr_built, seller_type, ceiling_height) 
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) 
             ON CONFLICT (listing_id) DO NOTHING`,
            [listing_id, date,city, address, price, bedrooms, bathrooms, sqr_meters, floor, yr_built, seller_type, ceiling_height, ]
        );
        console.log(`Inserted: ${listing_id} - ${price} KZT`);
    } catch (error) {
        console.error(`Error inserting listing ${listing_id}:`, error.message);
    }
}

async function scrapeListings(url) {
    const browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    const listings = await page.evaluate(() => {
        function extractListingData(card) {
            const titleElement = card.querySelector(".a-card__title");
            const title = titleElement ? titleElement.innerText.trim() : "";
    
            // Extract city
            const cityElement = card.querySelector(".a-card__stats-item");
            const city = cityElement ? cityElement.innerText.trim() : null;
    
            // Extract date
            const dateElement = card.querySelectorAll(".a-card__stats-item")[1]; // Second item
            const rawDate = dateElement ? dateElement.innerText.trim() : null;
    
            function convertDate(rawDate) {
                if (!rawDate) return null;
                const months = {
                    "янв.": "01", "фев.": "02", "мар.": "03",
                    "апр.": "04", "май": "05", "июн.": "06",
                    "июл.": "07", "авг.": "08", "сен.": "09",
                    "окт.": "10", "ноя.": "11", "дек.": "12"
                };
                const [day, monthText] = rawDate.split(" ");
                const month = months[monthText.toLowerCase()] || "01";
                return `2025-${month}-${day.padStart(2, "0")}`; // Format YYYY-MM-DD
            }
    
            const date = convertDate(rawDate);
    
            // Extract address
            const addressElement = card.querySelector(".a-card__subtitle");
            const address = addressElement ? addressElement.innerText.trim() : null;
    
            const idElement = card.querySelector("a");
            const listing_id = idElement ? idElement.href.match(/(\d+)$/)?.[1] || null : null;
    
            const priceElement = card.querySelector(".a-card__price");
            const priceText = priceElement ? priceElement.innerText.replace(/[^\d]/g, "") : "";
            const price = priceText ? parseInt(priceText) : null;
    
            const descElement = card.querySelector(".a-card__text-preview");
            const desc = descElement ? descElement.innerText : "";
    
            // Extract floor number (e.g., "6/9" -> 6, or "6" -> 6)
            const floorMatch = title.match(/(\d+)\/?\d*/);
            const floor = floorMatch ? parseInt(floorMatch[1]) : null;
    
            // Extract bedrooms (from title, assuming format like "3-комнатная")
            const bedroomsMatch = title.match(/(\d+)-комнатная/);
            const bedrooms = bedroomsMatch ? parseInt(bedroomsMatch[1]) : null;
    
            // Extract square meters
            // Extract square meters (e.g., "38.9 м²")
                const sqrMatch = title.match(/(\d+\.?\d*)\s*м²/);
                const sqr_meters = sqrMatch ? parseFloat(sqrMatch[1]) : null;

            // Extract year built
            const yearMatch = desc.match(/(\d{4}) г\.п\./);
            const yr_built = yearMatch ? parseInt(yearMatch[1]) : null;
    
            // Extract bathrooms type
            let bathrooms = 1;
            if (desc.includes("санузел раздельный")) bathrooms = 2;
            if (desc.includes("санузел совмещенный")) bathrooms = 1;
    
            // Extract ceiling height
            const ceilingMatch = desc.match(/потолки\s*(\d+\.\d+|\d+)м/);
            const ceiling_height = ceilingMatch ? parseFloat(ceilingMatch[1]) : null;

            // Extract seller type
        const sellerElement = card.querySelector(".a-card__owner-label div.label");
        const seller_type = sellerElement ? sellerElement.innerText.trim() : "Застройщик"; // Default to private seller

    
    
            return { listing_id, date, city, address, price, bedrooms, bathrooms, sqr_meters, floor, yr_built, seller_type,ceiling_height };
        }
    
        return Array.from(document.querySelectorAll(".a-card")).map(extractListingData);
    });
    
    
    

    await browser.close();

    // Insert data into PostgreSQL
    for (const listing of listings) {
        if (listing.listing_id) {
            await insertListingIntoDB(listing);
        }
    }
}

async function startScraping() {
    const baseURL = "https://krisha.kz/prodazha/kvartiry/";
    const maxPages = 1000;

    for (let i = 200; i <= maxPages; i++) {
        const url = i === 1 ? baseURL : `${baseURL}?page=${i}`;
        await scrapeListings(url);
    }
}

startScraping();




