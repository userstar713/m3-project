// K&L SCRAPER
var request = require('request');
var cheerio = require('cheerio');
var async = require('async');
var utils = require('.././utils');
var _ = require('underscore')._;
var S = require('string');


// This will be prepended to the individual product links
var baseURL = "http://www.klwines.com";
// This URL contains number of wines we want to get
var numWinesQuery = "http://www.klwines.com/content.asp?N=81++82&display=1&Nr=AND%28p_giftBoxYN%3AN%2COR%28OutofStock%3AN%2CInventory+Location%3ASpecial+Order%29%29&Ns=p_lotGeneratedFromPOYN%7C0%7C%7Cp_vintage%7C1";
// This URL gets a 500 product page of results starting at {{startingPos}}
var pagesOfWinesQuery = "http://www.klwines.com/content.asp?No={{startingPos}}&N=57&display={{productsPerPage}}&Nr=AND%28p_giftBoxYN%3AN%2COR%28OutofStock%3AN%2CInventory+Location%3ASpecial+Order%29%29&Ns=p_lotGeneratedFromPOYN%7C0%7C%7CQtySoldLast30%7C1%7C%7CQtySoldLifetime%7C1";

// Enable cookies for scraper
var myrequest = request.defaults({jar: true});

// Start at the first product and get 500 per page.
var productsPerPage = 10;
pagesOfWinesQuery = pagesOfWinesQuery.replace("{{productsPerPage}}", productsPerPage.toString());

var startingPos = 0;

var db;


exports.process = function (testFlag, database, brands, regions, callback) {

    db = database;

    // if testFlag is set then return the last-run stored in the source_ collection
    if (testFlag) {
        db.collection('source_klwines').find({}).toArray(function (err, prods) {
            return callback(null, prods);
        });
    }
    else {
        db.collection('source_klwines').drop(function (err, reply) {

            console.log("STARTING SCRAPING: K&L");

            getProductCount(function (productCount) {

                getAllProductLinksFromFeed(productCount, function (productList) {

                        // We now have an array of objects with links to the detail pages. We will look at each one to get detail
                        // The product object itself will be updated
                        var i = 0;
                        async.forEachLimit(productList, 1, function (product, productCb) {
//                                console.log("KL Wines: product "+ i++ +" of "+productList.length);

                                // If we don't already have the link in source_input, then scrape that one product
                                db.collection('source_input').findOne({link: product.link}, function (err, doc) {

                                    // If we already have it, then just move on; otherwise it's a new product so go get its data
                                    if (doc) {
                                        db.collection('source_klwines').save(product, function () {
                                            productCb();
                                        });
                                    }
                                    else {
                                        getProductAtLink(product, function (product) {
                                            if (product) {
                                                db.collection('source_klwines').save(product, function () {
                                                    productCb();
                                                });
                                            }
                                            else {
                                                productCb();            // Don't save a null product to source collection
                                            }

                                        });
                                    }
                                });
                            }
                            , function () {
                                console.log("LEAVING K&L SCRAPER");
                                productList = _.compact(productList);       // just in case we had to null something out because of bad data
                                return callback(null, productList);
                            })
                    }
                );
            });
        });
    }
};

exports.getProductAtLink = function (product, cb) {
    return getProductAtLink(product, cb);
}

// GET THE DETAIL FOR ONE PRODUCT AND PUT THAT IN THE PRODUCT OBJECT
function getProductAtLink(product, cb) {

    console.log("KLWINES: PROCESSING LINK: " + product.link);

    myrequest({
        url: product.link,
        headers: {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36"}
    }, function (error, response, body) {
        if (error || response.statusCode != 200) {
            console.log("CAN'T GET PRODUCT AT LINK: " + product.link + ", err=" + error);
            cb(product);
        }
        else {
            //console.log(body);

            // Wrap all the synchronous code in a try/catch block
            try {
                $ = cheerio.load(body);

                // Name
                product.full_wine_name = $('h1').text().trim();

                // Now pull out anything in parens (e.g., (Previously X))
                product.full_wine_name = product.full_wine_name.replace(/\(.+\)/, "");


                // Image string
                var str = $('div.col-a').find('div.productImg').find('a').find('img').first().attr('src');
                product.image = baseURL + str;


                // Price
                str = $('div.result-info').find('span.global-pop-color').find('strong').text();
                var price = (/\$(\d.+)/.exec(str));
                // It's possible that price is "Hidden" when not logged in as user
                if (price) {
                    price = price[0];
                    price = parseFloat(utils.cleanUpNumberString(price));
                    product.actual_price = price;
                }
                else
                    product.actual_price = "ERROR READING PRICE";


                // Inventory - For now, we're just going to add up all inventory for all locations. In future we need to figure out
                // how to split these by location
                var qoh = 0;
                $('div.inventory').find('div.column').find('tr').each(function (i, elem) {
                    var location = $(this).find('td').first().text().trim();
                    var quantity = $(this).find('td').last().text().trim();
                    if (quantity.substr(0, 1) == ">")
                        quantity = quantity.substr(1);
                    qoh += parseInt(quantity);
                });
                product.qoh = qoh;

                var foundVarietal = false;                  // We track whether we found a varietal to identify beer (which has none)
                // Varietal, Country, Region, Alcohol
                $('div.addtl-info-block').find('tr').each(function (i, elem) {

                    var title = $(this).find('td.detail_td1').text().trim();
                    // Alcohol Content has no H3 tag
                    if (title == "Alcohol Content (%):")
                        var value = $(this).find('td.detail_td').text().trim();
                    else
                        var value = $(this).find('td.detail_td').find('h3').text().trim();

                    var red_white_type = "";
                    if (title == "Varietal:") {
                        // Get rid of trailing "and ...." if it exists in string
                        foundVarietal = true;
                        var pos = value.indexOf(" and ");
                        if (pos > -1)
                            value = value.substr(0, pos);
                        if (value.indexOf("Other White") > -1) {
                            value = "";
                            red_white_type = ["white", "sparkling"];
                            // red_white_type = "white"; // Should not set to white because
                        }
                        else if (value.indexOf("Other Red") > -1) {
                            value = "";
                            red_white_type = "red";
                        }

                        product.name_varietal = value;
                        product.varietals = [];
                        if (value.length > 0) {
                            product.varietals.push(value);
                        }
                        if (red_white_type.length > 0)
                            product.red_white_type = red_white_type;
                    }
                    else if (title == "Country:")
                        product.country = value;
                    else if (title == "Sub-Region:")
                        product.region = value;
                    else if (title == "Specific Appellation:")  // Overwrite sub-region if we have more specific region
                        product.region = value;
                    else if (title == "Alcohol Content (%):")
                        product.alcohol = value;
                    // console.log(title + " " + value);

                });

                // Bottle Size
                var bottle_size = 750;
                if (product.full_wine_name.search(/(187\s?ml)/i) > -1)
                    bottle_size = 187;
                else if (product.full_wine_name.search(/375\s?ml/i) > -1)
                    bottle_size = 375;
                else if (product.full_wine_name.search(/1.5\s?l/i) > -1)
                    bottle_size = 1500;
                else if (product.full_wine_name.search(/3.0\s?l/i) > -1)
                    bottle_size = 3000;
                product.bottle_size = bottle_size;


                // Process reviews
                var reviews = [];
                var points_reviewer = $('div.result-desc').find('span.H2ReviewNotes').toArray();
                var review_texts = $('div.result-desc').find('p').toArray();
                for (var i = 0; i < points_reviewer.length; i++) {

                    var temp = points_reviewer[i].children[0].data.trim();

                    // K&L reviews are formatted differently, so skip over them (they're probably always last anyway)
                    if (temp.indexOf("K&L") > -1) {
                        var score_str = "";
                        var score = null;
                        var reviewer_name = "K&LNotes";
                        var review = review_texts[i].children[0].data.trim();
                        reviews.push({
                            reviewer_name: reviewer_name,
                            score: score,
                            score_str: score_str,
                            review_text: review
                        });
                    }
                    else {
                        var score_str = temp.substr(0, 6).trim();
                        var score = (score_str.indexOf("-") == -1) ? parseInt(temp) : parseInt(temp.substr(3));
                        var reviewer_name = temp.substr(temp.indexOf("points") + 6).trim();
                        // Simplify reviewer name if required
                        reviewer_name = (reviewer_name.indexOf("Wine Advocate") > -1) ? "Wine Advocate" :
                            (reviewer_name.indexOf("Tanzer") > -1) ? "Stephen Tanzer" : reviewer_name;
                        var review = review_texts[i].children[2].data.trim();
                        reviews.push({
                            reviewer_name: reviewer_name,
                            score: score,
                            score_str: score_str,
                            review_text: review
                        });
                    }
                }
                product.reviews = reviews;

                // Sometimes if there are no reviews, there is a retailer write-up
                var str = $('div.result-desc').find('p').text().trim();
                if (str && reviews.length == 0)
                    reviews.push({reviewer_name: "K&LNotes", score: null, score_str: "", review_text: str});

                // Get Vintage
                var vintage = parseInt(product.full_wine_name.substr(0, 4));
                if (vintage > 0)
                    product.vintage = vintage;
                else
                    product.non_vintage = true;

                delete product.name_varietal; // don't need to return this string as data is in array

                // If there is no varietal reference or it's hard alcohol, null out the product (that we'll remove later)
                if ((!foundVarietal && product.alcohol > 19) || (product.name_varietal > 0 && "vodka cordial eau de vie malt other distilled spirits brandy cognac gin irish rum rye scotch whiskey".indexOf(product.name_varietal.toLowerCase()) > -1))
                    product = null;
                // Also null out if not a 750
                if (product.bottle_size != 750)
                    product = null;

                cb(product);
            }
            catch (err) {

                // Flag this record as something funky
                if (product) {
                    product.error = err;
                    console.error("(CATCH) ERROR PROCESSING K&L LINK: " + product.link);
                }
                console.error("Error: " + err);
                cb(product);
            }

        }
    });
}


// GET NUMBER OF PRODUCTS WE WILL GET DETAILS FOR
function getProductCount(cb) {

    return cb(9999);    // don't need this function if we're getting from feed

    // First login
    var id = "winetron";
    var pwd = "ilovewine";

    // Note that the first request just sets some cookies that are used on the second request
    myrequest({url: 'http://www.klwines.com/'}, function (err, response, body) {

        myrequest({
            url: 'https://www.klwines.com/secshopper/lookup.asp',
            method: "POST",
            followAllRedirects: true,
            secureProtocol: 'SSLv3_method',
            form: {onlineid: id, password: pwd, reqemail: "Y", submit: "1", x: "0", y: "0"},
            headers: {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36"}
        }, function (error, response, body) {

            myrequest({
                url: numWinesQuery,
                headers: {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36"}
            }, function (error, response, body) {
                if (error || response.statusCode != 200) {
                    console.log(error);
                    cb(-1);
                }
                else {
                    // Search string is: "Your search returned 1234 results
                    var regex = /Your search returned (\d+) results/i;
                    var count = parseInt((regex.exec(body))[1]);
                    console.log("K&L Product Count: " + count);
                    if (count < 1000)
                        return cb(0);
                    cb(count);
                }
            });

        });
    })


}

// FILL THE ARRAY WITH PRODUCT OBJECTS THAT CONTAIN THE LINK & PRODUCT NAM
function getAllProductLinks(productCount, cb) {

    // return productList as objects with "link" property set
    var productList = [];

    var numPages = Math.floor((productCount - 1) / productsPerPage) + 1;
    // numPages = 1;  // REMOVE THIS

    // We'll create an array of pages - each just has the offset
    var pages = [];
    for (var i = 0; i < numPages; i++) {
        pages.push(i * productsPerPage);
    }

    async.eachLimit(pages, 1, function (page, pageCb) {

        var url = pagesOfWinesQuery.replace("{{startingPos}}", (i * page).toString());
        myrequest({
                url: url,
                headers: {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36"}
            },
            function (error, response, body) {
                if (error || response.statusCode != 200) {
                    console.log("ERROR READING K&L PRODUCT LIST PAGE, err=" + error);
                    cb([]);
                }
                else {
                    $ = cheerio.load(body);
                    var links = [];
                    $('div.results-block').find('div.result').each(function (i, elem) {
                        var productName = $(this).find('a').attr('title');
                        var link = $(this).find('a').attr('href');
                        if (link)
                            links.push({"full_wine_name": productName, link: baseURL + "/" + link});
                    });
                    productList = productList.concat(links);
                    pageCb();
                }
            });
    }, function () {
        productList = _.compact(productList);

        cb(productList);
    });

}


function getAllProductLinksFromFeed(productCount, cb) {

    // return productList as objects with "link" property set
    // we ignore productCount
    var linkList = [];

    console.log("READING K&L FEED");
    myrequest({
            url: "http://www.klwines.com/exports/Winesearcher.txt",
            headers: {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36"}
        },
        function (error, response, body) {
            if (error || response.statusCode != 200) {
                console.log("ERROR READING K&L PRODUCT FEED, err=" + error);
                cb([]);
            }
            else {

                var rows = body.split("\r\n");
                for (var i = 1; i < rows.length; i++) {
                    // delete this row below
                    // for (var i = 5005; i < 5020; i++) {

                    // ignore if beer or spirits or other stuff
                    if ((rows[i].indexOf("Wine - Sparkling") == -1) && (rows[i].indexOf("Wine - Red") == -1)
                        && (rows[i].indexOf("Wine - White") == -1) && (rows[i].indexOf("Wine - Rose") == -1))
                        continue;

                    // Ignore if they are not a 750
                    if ((rows[i].toLowerCase().indexOf("375ml") != -1) || (rows[i].toLowerCase().indexOf("1.5l") != -1) ||
                        (rows[i].toLowerCase().indexOf("3.0l") != -1) || (rows[i].toLowerCase().indexOf("3l") != -1) ||
                        (rows[i].toLowerCase().indexOf("6l") != -1) || (rows[i].toLowerCase().indexOf("12l") != -1))
                        continue;

                    // extract qoh and link and name
                    var vals = rows[i].match(/(\$\d+.\d+|sku=\d+)/g);
                    if (vals.length != 2)
                        continue;
                    if (vals[0].substr(0, 1) != "$")
                        continue;
                    if (vals[1].substr(0, 3) != "sku")
                        continue;

                    var priceStr = utils.cleanUpNumberString(vals[0]);
                    price = parseFloat(priceStr);
                    sku = "http://www.klwines.com/detail.asp?" + vals[1];

                    linkList.push({link: sku, actual_price: price, qoh: 12});
                }

                cb(linkList);            // We should have all of the links now
            }
            // Convert to JSON and extract sku string and price

        });
}
