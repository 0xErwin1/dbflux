// Synthetic demo data for the documentation screenshots, run with mongosh
// against the `shop` database. Values come from fixed lists and arithmetic on
// the document index, so every run inserts the same documents.

const shop = db.getSiblingDB("shop");

const authors = [
  { name: "Ada Alvarez", country: "Argentina", verified: true },
  { name: "Bruno Becker", country: "Germany", verified: false },
  { name: "Carla Costa", country: "Brazil", verified: true },
  { name: "Diego Dubois", country: "Canada", verified: true },
  { name: "Elena Evans", country: "United States", verified: false },
  { name: "Kenji Hayashi", country: "Japan", verified: true },
];

const cities = [
  { city: "Buenos Aires", coordinates: [-58.3816, -34.6037] },
  { city: "Berlin", coordinates: [13.405, 52.52] },
  { city: "Sao Paulo", coordinates: [-46.6333, -23.5505] },
  { city: "Toronto", coordinates: [-79.3832, 43.6532] },
  { city: "Seattle", coordinates: [-122.3321, 47.6062] },
  { city: "Osaka", coordinates: [135.5023, 34.6937] },
];

const products = ["KB-101", "MS-210", "MN-270", "HD-500", "CM-080", "DK-900", "SS-1TB", "CH-700"];
const tagPool = ["fast-shipping", "gift", "value", "quality", "packaging", "support", "durable", "quiet"];
const titles = [
  "Exactly what I needed",
  "Solid build quality",
  "Good, but the manual is thin",
  "Arrived a day early",
  "Would buy again",
  "Works out of the box",
];

const reviewBase = Date.UTC(2025, 0, 6, 10, 0, 0);
const reviews = [];

for (let index = 0; index < 60; index++) {
  const author = authors[index % authors.length];
  const place = cities[(index * 5) % cities.length];

  reviews.push({
    _id: `rev-${String(index + 1).padStart(4, "0")}`,
    product_sku: products[(index * 3) % products.length],
    rating: 1 + ((index * 7) % 5),
    title: titles[index % titles.length],
    author: {
      name: author.name,
      country: author.country,
      verified: author.verified,
    },
    location: {
      city: place.city,
      geo: { type: "Point", coordinates: place.coordinates },
    },
    tags: [tagPool[index % tagPool.length], tagPool[(index * 3 + 1) % tagPool.length]],
    helpful_votes: (index * 13) % 40,
    created_at: new Date(reviewBase + index * 37 * 3600 * 1000),
  });
}

const eventTypes = ["page_view", "add_to_cart", "checkout_started", "order_placed", "search"];
const eventBase = Date.UTC(2025, 2, 1, 0, 0, 0);
const events = [];

for (let index = 0; index < 120; index++) {
  const type = eventTypes[(index * 3) % eventTypes.length];

  events.push({
    _id: `evt-${String(index + 1).padStart(5, "0")}`,
    type,
    session_id: `sess-${String(1 + (index % 17)).padStart(3, "0")}`,
    at: new Date(eventBase + index * 11 * 60 * 1000),
    payload:
      type === "search"
        ? { query: tagPool[index % tagPool.length], results: (index * 7) % 50 }
        : { sku: products[index % products.length], quantity: 1 + (index % 3) },
  });
}

shop.reviews.drop();
shop.events.drop();
shop.reviews.insertMany(reviews);
shop.events.insertMany(events);
shop.reviews.createIndex({ product_sku: 1, rating: -1 });
shop.events.createIndex({ type: 1, at: -1 });
