var SlackBot = require('slackbots');
var request = require('request');

// create a bot
var bot = new SlackBot({
    token: 'xoxb-48608617153-p0s6cAmkMCAYG0pkaX3tRyzx', // Add a bot https://my.slack.com/services/new/bot and put the token
    name: 'Librarian'
});

var params = {
    icon_emoji: ':cat:'
};

var configString = 'tags=story&query=spark&numericFilters=num_comments%3E5&numericFilters=created_at_i>2016-06-06T20:03:34.000Z'

var searchTerms = ['spark'];
var minComments= 5;
var timestamp = '2016-06-06T20:03:34.000Z';

function callHn(url) {
    bot.postMessageToUser('ilganeli', "Calling API with " + url, params);
    request(url, function (error, response, body) {
            if (!error && response.statusCode == 200) {
                console.log(body) // Print the google web page.

        bot.postMessageToUser('ilganeli', parseHNObject(body), params);
            }
            }}

/**
 * Takes the jsonObject returned by the HN Angola API
 * And converts it to the following format: [{url, title}, {url, title}, ...]
 */
function parseHNObject(jsonObject){
    parsed_data = []
    hit_count = jsonObject["hits"].length

    // Iterate over every hit
    for (i = 0; i < hit_count; i++) {

    // Ignore ask HN's
      if(jsonObject["hits"][i]["url"] != null){
          parsed_data.push({
        "title": jsonObject["hits"][i]["title"],
        "url": jsonObject["hits"][i]["url"]
          })
      }
    }

    return parsed_data;
}

function createHNURL(){
    base_url    = "http://hn.algolia.com/api/v1/search_by_date?tags=story"
    query_url   = "&query="+searchTerms.join()
    comment_url = "&numericFilters=num_comments>"+parseInt(minComments)
    time_url    = "&numericFilters=created_at_i>"+timestamp

    return base_url + query_url + comment_url + time_url
}

bot.on('start', function() {
    // more information about additional params https://api.slack.com/methods/chat.postMessage

    // define channel, where bot exist. You can adjust it there https://my.slack.com/services

    // var myVar = setInterval(myTimer, 10000);

    function myTimer() {
        var d = new Date();
        callHn(configString)
    }
});

bot.on('message', function(message) {
   if (typeof message.text != 'undefined') {
     if (message.text.startsWith("Search for")) {
       var splitArr = message.text.split("Search for ")
       searchTerms = splitArr[1].split(",")
       var url = createHNURL(searchTerms);
       callHn(url)
     }
  }
});
