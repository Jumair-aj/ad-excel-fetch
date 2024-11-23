const { google } = require("googleapis");
const cron = require("node-cron");
const axios = require("axios");
const { parseStringPromise } = require("xml2js");
const express = require('express');
const app = express();
const port = process.env.PORT || 3000;

app.use(express.json());


const SHEET_ID = "1rJg9dmsQGKAbPtZWEoOgVaLiaNg3HVZRNVJRyt4k9f0"; // The ID of your Google Sheet
const RANGE = "Sheet1!A:Z"; // This tells Google Sheets to append to the next available row

// Full API list with dynamic dates
// Function to format dates in YYYY-MM-DD format
function formatDate(date) {
    // Ensure date is a Date object
    const validDate = new Date(date);

    // Check if the date is valid
    if (isNaN(validDate)) {
        throw new Error('Invalid Date');
    }

    const year = validDate.getFullYear();
    const month = String(validDate.getMonth() + 1).padStart(2, '0'); // Months are 0-indexed, so add 1
    const day = String(validDate.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

// Get today's date and the date 7 days ago
const today = new Date();
const sevenDaysAgo = new Date();
sevenDaysAgo.setDate(today.getDate() - 7);

// Format the dates
const formattedToday = formatDate(today);
const formattedSevenDaysAgo = formatDate(sevenDaysAgo);

// Replace the date values in the API URLs dynamically
const apiEndpoints = [
    { name: "DoByAI", url: `https://api.bc-sys.com/ssp_xml?platform=mobicolor&endpoint=MonMa_Inap_Video_US_EAST&token=06c3bf514bc402a86e4faf12764763b8&start=${formattedSevenDaysAgo}&end=${formattedToday}` },
    { name: "DoByAI", url: `https://api.bc-sys.com/ssp_xml?platform=mobicolor&endpoint=MonMa_CTV_US_EAST&token=06c3bf514bc402a86e4faf12764763b8&start=${formattedSevenDaysAgo}&end=${formattedToday}` },
    { name: "DoByAI", url: `https://dobyai.rpt.rixengine.com/ssp/api/v2?x-userid=37159&x-authorization=fdced8e95b72be0581b05ca85adbcfa1&start_date=${formattedSevenDaysAgo}&end_date=${formattedToday}` },
    { name: "DoByAI", url: `https://dobyai.rpt.rixengine.com/ssp/api/v2?x-userid=37160&x-authorization=b7118165fd1ba5ba0ec08d8d41cd5ad0&start_date=${formattedSevenDaysAgo}&end_date=${formattedToday}` },
    { name: "Winkleads BC EPs", url: `https://api.bc-sys.com/ssp_xml?platform=winkleads&endpoint=MonetizeMatrix_BC_ADT_HQ_PX_CTV_RTB_05Sep24_US_EAS&token=9e6667c94ba52789187cb9cbef7ae7fa&start=${formattedSevenDaysAgo}&end=${formattedToday}` },
    { name: "Winkleads BC EPs", url: `https://api.bc-sys.com/ssp_xml?platform=winkleads&endpoint=MonetizeMatrix_BC_ADT_HQ_PX_IAV_RTB_05Sep24_US_EAS&token=9e6667c94ba52789187cb9cbef7ae7fa&start=${formattedSevenDaysAgo}&end=${formattedToday}` },
    { name: "HAXMedia", url: `http://stats.ortb.net/v1/stats?clientKey=hax&pubId=656243&secretKey=e298109f3831af022d8a9ae9fd9b922a&breakdown=DATE&output=json&startDate=${formattedSevenDaysAgo}&endDate=${formattedToday}&timezone=UTC&metrics=OPPORTUNITIES,IMPRESSIONS,PUB_PAYOUT` },
    { name: "HAXMedia", url: `https://new-api.smart-hub.io/api/report/ssp?token=7f50c4f6291716f4c4a71f4a672c378c&endpoint=125011&from=${formattedSevenDaysAgo}&to=${formattedToday}` },
    { name: "Take1", url: `http://stats.ortb.net/v1/stats?clientKey=tk&pubId=34889181&secretKey=8f7637de0e83c58bce82680cfa342224&breakdown=DATE&output=json&startDate=${formattedSevenDaysAgo}&endDate=${formattedToday}&timezone=UTC&metrics=OPPORTUNITIES,IMPRESSIONS,PUB_PAYOUT` },
    { name: "Take1", url: `http://stats.ortb.net/v1/stats?clientKey=tk&pubId=34889182&secretKey=c3a6a7a39d558d5a8fcd7cc79cbe868c&breakdown=DATE&output=json&startDate=${formattedSevenDaysAgo}&endDate=${formattedToday}&timezone=UTC&metrics=OPPORTUNITIES,IMPRESSIONS,PUB_PAYOUT` },
    { name: "Take1", url: `http://stats.ortb.net/v1/stats?clientKey=tk&pubId=34889183&secretKey=48cec5dbe7eb199f270c73f81b037cd2&breakdown=DATE&output=json&startDate=${formattedSevenDaysAgo}&endDate=${formattedToday}&timezone=UTC&metrics=OPPORTUNITIES,IMPRESSIONS,PUB_PAYOUT` },
    { name: "Bizzclick", url: `https://ex.blasto.ai/api/v1/daily-stats/ssp?endpoint=BC_monetize_matrix_ctv_US_EAST&token=JT8YsQFcFCiksWW31b8h&from=${formattedSevenDaysAgo}&to=${formattedToday}` },
    { name: "Bizzclick", url: `https://ex.blasto.ai/api/v1/daily-stats/ssp?endpoint=BC_monetize_matrix_ia_video_US_EAST&token=PSaxvz3X7ChxVBNU9kNn&from=${formattedSevenDaysAgo}&to=${formattedToday}` },
    { name: "Bizzclick", url: `https://ex.blasto.ai/api/v1/daily-stats/ssp?endpoint=BC_monetize_matrix_ia_video_nov24_US_EAST&token=VyjHEzx3ICZ2MAE67YEb&from=${formattedSevenDaysAgo}&to=${formattedToday}` },
    { name: "Bizzclick", url: `https://ex.blasto.ai/api/v1/daily-stats/ssp?endpoint=BC_monetize_matrix_ctv_no_limits_US_EAST&token=fNBqAD08BguFA2WIk4y0&from=${formattedSevenDaysAgo}&to=${formattedToday}` },
    { name: "Kunvertads", url: `https://api.bc-sys.com/ssp_xml?platform=kunvertads&endpoint=monetizematrix_ADT_IAVIDEO_041024_US_EAST&token=fd6aaaca1cee849348e970732a4caa6d&start=${formattedSevenDaysAgo}&end=${formattedToday}` },
    { name: "Westcom WO EP", url: `http://api.westcom.live/ssp/financial/?apiKey=7f81cbca7e751f7a5446d39ed8c35886&from=${formattedSevenDaysAgo}&endpoint_id=5132` },
    { name: "Adokut", url: `http://login.adokutrtb.com/publisher/svc?action=outcsv&login=MonetizeMatrix&password=d6WRZ8kX&channel=ZoneReports&dim=date&f.zone=238690&f.date=${formattedSevenDaysAgo}_${formattedToday}&columns=date,rtb_pub_requests,rtb_pub_wins,rtb_pub_wins_price,rtb_pub_gross_impressions,rtb_pub_impressions,rtb_pub_clicks,rtb_pub_gross,rtb_rem_impressions,rtb_rem_clicks,rtb_pub_revenue&appType=CPM` },
    { name: "Adokut", url: `http://reporting.adokutrtb.com/api/report/ssp?token=08db50994ddae71376b6f4795a4c9cc1&endpoint=125069&from=${formattedSevenDaysAgo}&to=${formattedToday}` },
    { name: "Adbite", url: `https://ssp.adtelligent.com/api/statistics/ssp2.xml?report=date&auth_token=178cb8684e3f9dfe2733da46d72efa02&date_from=${formattedSevenDaysAgo}&date_to=${formattedToday}&source=914831` },
    { name: "Adbite", url: `https://adbite-api.bidscube.com/ssp_xml.php?endpoint=MonetizeMatrix_Video_ADT_041124_US_EAST&apikey=TvoMm3JMgWldkvYq3wDi&start=${formattedSevenDaysAgo}&end=${formattedToday}` },
    { name: "Adbite", url: `https://adbite-api.bidscube.com/ssp_xml.php?endpoint=MonetizeMatrix_CTV_ADT_041124_US_EAST&apikey=VXXafwjSev9KFe9cHXQT&start=${formattedSevenDaysAgo}&end=${formattedToday}` },
    // { name: "Adbite", url: `http://api.adbite.live/ssp/financial/?apiKey=943042c1f4b399e3646c3241ee4dfb74&from=${formattedSevenDaysAgo}&endpoint_id=4963` },
    { name: "Adbite", url: `https://ssp.adtelligent.com/api/statistics/ssp2.xml?report=date&auth_token=178cb8684e3f9dfe2733da46d72efa02&date_from=${formattedSevenDaysAgo}&date_to=${formattedToday}&source=914180` },
    { name: "Adbite", url: `https://ssp.adtelligent.com/api/statistics/ssp2.xml?report=date&auth_token=178cb8684e3f9dfe2733da46d72efa02&date_from=${formattedSevenDaysAgo}&date_to=${formattedToday}&source=914181` },
    { name: "Adbite", url: `https://adbite-api.bidscube.com/ssp_xml.php?endpoint=MonetizeMatrix_CTV_ADT_131124_US_EAST&apikey=bBGmPG7uvqE1fk5tmITB&start=${formattedSevenDaysAgo}&end=${formattedToday}` },
    { name: "Adbite", url: `https://adbite-api.bidscube.com/ssp_xml.php?endpoint=MonetizeMatrix_Video_ADT_131124_US_EAST&apikey=onHyyopDyfEPpNNgWzzC&start=${formattedSevenDaysAgo}&end=${formattedToday}` },
    { name: "Intellectscoop", url: `http://api.intellectscoop.live/ssp/financial/?apiKey=57a353134f85fbb6034162d4926a7ca9&from=${formattedSevenDaysAgo}&endpoint_id=4717` },
    { name: "Intellectscoop ", url: `http://api.intellectscoop.live/ssp/financial/?apiKey=57a353134f85fbb6034162d4926a7ca9&from=${formattedSevenDaysAgo}&endpoint_id=4716` },
    { name: "Kodio", url: `https://adelion.com/api/ortb/stats/publisher/rtb?username=monetizeMatrix&auth=ZpSRQ0&zone=2043&date_from=${formattedSevenDaysAgo}&date_to=${formattedToday}&columns=DateStats,Requests,Wins,Bills,NetImpressions,Spent` },
    { name: "Kodio", url: `https://adelion.com/api/ortb/stats/publisher/rtb?username=monetizeMatrix&auth=ZpSRQ0&zone=2091&date_from=${formattedSevenDaysAgo}&date_to=${formattedToday}&columns=DateStats,Requests,Wins,Bills,NetImpressions,Spent` },
    { name: "Adxfactory", url: `http://login.adxfactory.com/publisher/svc?action=outcsv&login=Monetizematrix&password=BJaa2Wdj&channel=ZoneReports&dim=date&f.zone=239413&f.date=${formattedSevenDaysAgo}_${formattedToday}&columns=date,rtb_pub_requests,rtb_pub_wins,rtb_pub_wins_price,rtb_pub_gross_impressions,rtb_pub_impressions,rtb_pub_clicks,rtb_pub_gross,rtb_rem_impressions,rtb_rem_clicks,rtb_pub_revenue&appType=CPM` },
    { name: "Adxfactory", url: `http://login.adxfactory.com/publisher/svc?action=outcsv&login=Monetizematrix&password=BJaa2Wdj&channel=ZoneReports&dim=date&f.zone=239416&f.date=${formattedSevenDaysAgo}_${formattedToday}&columns=date,rtb_pub_requests,rtb_pub_wins,rtb_pub_wins_price,rtb_pub_gross_impressions,rtb_pub_impressions,rtb_pub_clicks,rtb_pub_gross,rtb_rem_impressions,rtb_rem_clicks,rtb_pub_revenue&appType=CPM` },
    { name: "Felixads", url: `http://api.felixads.live/ssp/financial/?apiKey=80fa0973b29abdf0be106b19a81c7f17&from=${formattedSevenDaysAgo}&endpoint_id=3696` },
    { name: "Felixads", url: `http://api.felixads.tv/api/report/ssp?token=0eea0a67bc0b91ff109b72baf0efc708&endpoint=124322&from=${formattedSevenDaysAgo}&to=${formattedToday}` },
    { name: "Felixads", url: `http://api.felixads.live/ssp/financial/?apiKey=80fa0973b29abdf0be106b19a81c7f17&from=${formattedSevenDaysAgo}&endpoint_id=3697` },
];


// Authenticate with Google
const auth = new google.auth.GoogleAuth({
    keyFile: "/etc/secrets/client_secret_1071299587561-5unmgmuvb4qtssd3qe2v11e5qj2ekts6.apps.googleusercontent.com.json", // Service account key
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

const sheets = google.sheets({ version: "v4", auth });

async function fetchData(api) {
    const API_URL = api.url; // Replace with your API
    try {
        const response = await axios.get(API_URL);
        const apiHandlers = {
            // Group APIs with similar response structures
            spendData: ['Westcom', 'Intellectscoop', 'Kodio'],
            pubPayout: ['Take1', 'HAXMedia'],
            revenue: ['Felixads'],
            xml: ['Winkleads', 'Adbite', 'Kunvertads', 'DoByAI', 'DoByAI'], // Added DoByAI to XML APIs
            json: ['Bizzclick', 'Adokut CTV'] // Added Bizzclick to JSON APIs
        };

        let formattedData;
        let jsonData;
        // Clone the formattedSevenDaysAgo date and increment by one day for each item

        const pastSevenDays = Array.from({ length: 7 }, (_, i) => {
            const date = new Date(formattedSevenDaysAgo);
            date.setDate(date.getDate() + i);
            return date.toISOString().split('T')[0]; // Format as "YYYY-MM-DD"
        });


        if (apiHandlers.spendData.some((name) => api.name.includes(name))) {
            if (response.data?.data && response.data.data.length > 0) {
                const responseData = response.data.data.map(({ date, spend }) => ({
                    date,
                    value: spend || 0,
                }));
                const responseMap = new Map(responseData.map(({ date, value }) => [date, value]));
                formattedData = pastSevenDays.map((day) => [day, responseMap.get(day) || 0]);
            } else {
                formattedData = pastSevenDays.map((day) => [day, 0]);
            }
        } else if (apiHandlers.pubPayout.some(name => api.name.includes(name))) {
            // Handle responses with pub_payout data
            if (api.name === 'HAXMedia' && response.data[0]?.revenue) {
                const responseData = response.data.map(({ date, revenue }) => [date, revenue]);
                const responseMap = new Map(responseData.map((item) => [item[0], item[1]]));
                formattedData = pastSevenDays.map((day) => [day, responseMap.get(day) || 0]);
            }
            else if (response.data?.body && response.data.body.length > 0) {
                const responseData = response.data.body.map(({ DATE, PUB_PAYOUT }) => ({
                    date: DATE,
                    value: PUB_PAYOUT || 0,
                }));
                const responseMap = new Map(responseData.map(({ date, value }) => [date, value]));
                formattedData = pastSevenDays.map((day) => [day, responseMap.get(day) || 0]);
            }
            else {
                formattedData = 'No revenue data found';
            }
        } else if (apiHandlers.xml.some(name => api.name.includes(name))) {
            if (response.headers["content-type"]?.includes("application/json")) {
                if (response.data?.data) {
                    if (api.name === 'Adbite') {
                        const responseData = response.data.data.map(({ date, spend }) => ({
                            date: date,
                            value: spend || 0,
                        }));
                        const responseMap = new Map(responseData.map(({ date, value }) => [date, value]));

                        formattedData = pastSevenDays.map((day) => [day, responseMap.get(day) || 0]);
                    } else {
                        const responseData = response.data.data.data.map(({ day, net_revenue }) => ({
                            date: day,
                            value: net_revenue || 0,
                        }));
                        const responseMap = new Map(responseData.map(({ date, value }) => [date, value]));
                        formattedData = pastSevenDays.map((day) => [day, responseMap.get(day) || 0]);
                    }
                } else {
                    formattedData = pastSevenDays.map((day) => [day, 0]);
                }
            }
            else {
                jsonData = await parseStringPromise(response.data, {
                    explicitArray: false, // Prevents creating arrays for each node
                    mergeAttrs: true,     // Merges attributes with the node
                });
                if (api.name === 'Adbite') {
                    // Extract relevant data for Adbite
                    if (jsonData?.data) {
                        const responseData = jsonData.data.map(({ date, spend }) => ({
                            date,
                            value: spend || 0,
                        }));
                        const responseMap = new Map(responseData.map(({ date, value }) => [date, value]));
                        formattedData = pastSevenDays.map((day) => [day, responseMap.get(day) || 0]);
                    }
                    else if (jsonData && jsonData.stats && jsonData.stats.breakdown) {
                        const breakdown = Array.isArray(jsonData.stats.breakdown)
                            ? jsonData.stats.breakdown
                            : [jsonData.stats.breakdown]; // Ensure it's an array

                        const responseData = breakdown.map(({ date, revenue }) => ({
                            date,
                            value: parseFloat(revenue) || 0,
                        }));
                        const responseMap = new Map(responseData.map(({ date, value }) => [date, value]));
                        formattedData = pastSevenDays.map((day) => [day, responseMap.get(day) || 0]);
                    }
                    else {
                        const items = jsonData.array.data.item || [];
                        formattedData = pastSevenDays.map((date, id) => {
                            return [date, parseFloat(items[id]?.ChannelRevenue)];
                        });
                    }
                } else if (jsonData && jsonData.stats && jsonData.stats.breakdown) {
                    const breakdown = Array.isArray(jsonData.stats.breakdown)
                        ? jsonData.stats.breakdown
                        : [jsonData.stats.breakdown]; // Ensure it's an array

                    const responseData = breakdown.map(({ date, revenue }) => ({
                        date,
                        value: parseFloat(revenue) || 0,
                    }));
                    const responseMap = new Map(responseData.map(({ date, value }) => [date, value]));
                    formattedData = pastSevenDays.map((day) => [day, responseMap.get(day) || 0]);
                }
                else {
                    formattedData = pastSevenDays.map((day) => [day, 0]);
                }
            }
        }
        else if (apiHandlers.json.some((name) => api.name.includes(name))) {
            // Handle JSON APIs
            if (response.data) {
                if (api.name === "Adokut CTV") {
                    const responseData = response.data.map(({ date, revenue }) => ({
                        date,
                        value: revenue || 0,
                    }));
                    const responseMap = new Map(responseData.map(({ date, value }) => [date, value]));
                    formattedData = pastSevenDays.map((day) => [day, responseMap.get(day) || 0]);
                } else {
                    const responseData = Object.entries(response.data).map(([date, { revenue }]) => ({
                        date,
                        value: revenue || 0,
                    }));
                    const responseMap = new Map(responseData.map(({ date, value }) => [date, value]));
                    formattedData = pastSevenDays.map((day) => [day, responseMap.get(day) || 0]);
                }
            } else {
                formattedData = pastSevenDays.map((day) => [day, 0]);
            }
        } else if (apiHandlers.revenue.some((name) => api.name.includes(name))) {
            // Handle revenue data
            if (response.data?.length > 0) {
                const responseData = response.data.map(({ date, revenue }) => ({
                    date,
                    value: revenue || 0,
                }));
                const responseMap = new Map(responseData.map(({ date, value }) => [date, value]));
                formattedData = pastSevenDays.map((day) => [day, responseMap.get(day) || 0]);
            } else {
                formattedData = pastSevenDays.map((day) => [day, 0]);
            }
        } else {
            formattedData = pastSevenDays.map((day) => [day, 0]);
        }
 

        return formattedData; // Ensure this returns the extracted data
    } catch (error) {
        console.log("error", api.name);
        console.log("error", api.url);
        console.error("Error fetching API data:", error.message);
    }
}
function main() {
    (async () => {
        console.log("Script started...");

        const aggregatedData = new Map(); // To store aggregated data

        // Fetch and aggregate data
        for (const api of apiEndpoints) {
            try {
                const data = await fetchData(api);
                if (data && Array.isArray(data)) {
                    if (!aggregatedData.has(api.name)) {
                        aggregatedData.set(api.name, new Map());
                    }
                    const dateRevenueMap = aggregatedData.get(api.name);

                    data.forEach(([date, revenue]) => {
                        const currentRevenue = dateRevenueMap.get(date) || 0;
                        dateRevenueMap.set(date, currentRevenue + (revenue || 0));
                    });
                }
            } catch (error) {
                console.error(`Error fetching data for API ${api.name}:`, error.message);
            }
        }

        console.log("Aggregated data:", aggregatedData);

        // Pass aggregated data to the dynamic appending function
        await updateSheetWithData(aggregatedData);

        console.log("Script ended...");
    })();
};


async function updateSheetWithData(aggregatedData) {
    // Step 1: Get existing sheet data
    const sheetData = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: RANGE,
    });

    const rows = sheetData.data.values || [];
    const headers = rows[0] || []; // First row as header
    const existingData = rows.slice(1); // Exclude header row

    // Step 2: Ensure all dates in `aggregatedData` are present in headers
    const allDates = new Set(headers.slice(1)); // Collect existing dates
    aggregatedData.forEach((dateRevenueMap) => {
        dateRevenueMap.forEach((_, date) => allDates.add(date));
    });

    const sortedDates = Array.from(allDates).sort(); // Sort dates for consistency
    const finalHeaders = ["Name", ...sortedDates];

    // Step 3: Prepare updated rows
    const updatedRows = existingData.map((row) => {
        const apiName = row[0];
        const dateRevenueMap = aggregatedData.get(apiName);

        if (!dateRevenueMap) return row; // Skip rows without matching data

        const updatedRow = [...row];
        sortedDates.forEach((date, index) => {
            if (dateRevenueMap.has(date)) {
                const colIndex = headers.indexOf(date);
                if (colIndex !== -1) {
                    updatedRow[colIndex] = dateRevenueMap.get(date) || 0; // Update revenue
                } else {
                    // Add new column for a date not in headers
                    updatedRow.push(dateRevenueMap.get(date) || 0);
                }
            }
        });

        return updatedRow;
    });

    // Step 4: Handle new API names (not in existing data)
    aggregatedData.forEach((dateRevenueMap, apiName) => {
        if (!existingData.some((row) => row[0] === apiName)) {
            const newRow = Array(finalHeaders.length).fill(0);
            newRow[0] = apiName; // Set API name

            sortedDates.forEach((date, index) => {
                const colIndex = finalHeaders.indexOf(date);
                if (colIndex !== -1) {
                    newRow[colIndex] = dateRevenueMap.get(date) || 0;
                }
            });

            updatedRows.push(newRow);
        }
    });

    // Step 5: Update the sheet
    try {
        // Update headers
        await sheets.spreadsheets.values.update({
            spreadsheetId: SHEET_ID,
            range: `${RANGE.split("!")[0]}!1:1`,
            valueInputOption: "RAW",
            resource: {
                values: [finalHeaders],
            },
        });

        // Update rows
        await sheets.spreadsheets.values.update({
            spreadsheetId: SHEET_ID,
            range: `${RANGE.split("!")[0]}!2:${updatedRows.length + 1}`,
            valueInputOption: "RAW",
            resource: {
                values: updatedRows,
            },
        });

        console.log("Google Sheet updated successfully.");
    } catch (error) {
        console.error("Error updating Google Sheet:", error.message);
    }
}


async function appendToSheetHorizontally(data, apiName) {
    try {
        // Fetch the current sheet data
        const sheetData = await sheets.spreadsheets.values.get({
            spreadsheetId: SHEET_ID,
            range: RANGE,
        });

        const rows = sheetData.data.values || [];
        const headers = rows[0] || []; // Extract headers (first row)
        const currentRows = rows.slice(1); // Extract data rows

        // Ensure headers include at least the "Name" column
        let updatedHeaders = headers.length ? headers : ["Name"];
        let currentDates = headers.slice(1); // Existing date headers

        // Add new dates to headers if not present
        if (Array.isArray(data) && data.length > 0) {
            const newDates = data.map(([date]) => date);
            newDates.forEach((date) => {
                if (!currentDates.includes(date)) {
                    updatedHeaders.push(date);
                }
            });
        }

        // Prepare a new or updated row for the API data
        const apiRow = Array(updatedHeaders.length).fill(""); // Initialize an empty row
        apiRow[0] = apiName; // Set the first column to the API name

        // Map data to appropriate columns based on the date
        if (Array.isArray(data)) {
            data.forEach(([date, value]) => {
                const colIndex = updatedHeaders.indexOf(date); // Find the column index
                if (colIndex !== -1) {
                    apiRow[colIndex] = value || 0; // Set the value (default to 0 if missing)
                }
            });
        }

        // Check if the name already exists in the sheet
        const nameRowIndex = currentRows.findIndex(row => row[0] === apiName);

        if (nameRowIndex !== -1) {
            // Replace the existing row
            const existingRow = rows[nameRowIndex + 1]; // Add 1 for header offset
            const updatedRow = [...existingRow]; // Start with existing row

            updatedHeaders.forEach((header, index) => {
                if (index === 0) return; // Skip "Name" column
                const newValue = apiRow[index];
                if (newValue !== "") updatedRow[index] = newValue; // Update with new data if available
            });

            // Update the specific row in the sheet
            await sheets.spreadsheets.values.update({
                spreadsheetId: SHEET_ID,
                range: `${RANGE.split("!")[0]}!${nameRowIndex + 2}:${nameRowIndex + 2}`,
                valueInputOption: "RAW",
                resource: {
                    values: [updatedRow],
                },
            });
            console.log(`Updated data for ${apiName} in Google Sheet.`);
        } else {
            // Append a new row
            await sheets.spreadsheets.values.append({
                spreadsheetId: SHEET_ID,
                range: RANGE,
                valueInputOption: "RAW",
                insertDataOption: "INSERT_ROWS", // Ensure the row is added without overwriting
                resource: {
                    values: [apiRow],
                },
            });
            console.log(`Appended data for ${apiName} to Google Sheet.`);
        }

        // Update headers if needed
        if (updatedHeaders.length > headers.length) {
            await sheets.spreadsheets.values.update({
                spreadsheetId: SHEET_ID,
                range: `${RANGE.split("!")[0]}!1:1`, // Update the first row (headers)
                valueInputOption: "RAW",
                resource: {
                    values: [updatedHeaders],
                },
            });
            console.log("Headers updated successfully.");
        }
    } catch (error) {
        console.error(`Error processing ${apiName}:`, error.message);
    }
}


// Expose an endpoint to trigger the function
app.post('/trigger-function', (req, res) => {
    try {
    main();
        res.status(200).send('Function triggered successfully!');
    } catch (error) {
        res.status(500).send('Error triggering function');
    }
});

app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});


console.log("Scheduler started...");
