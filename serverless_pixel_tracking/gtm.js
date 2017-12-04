/**
* Use this function as a value for the sendHitTask field in your UA tags
* to send your ga payload to two (or more) destinations (ex. GA/Big Query).
*
* @param {Object} model - this is a simple object that provides access
* to any of the fields defined in the Analytics.js Field Reference.
*
* See
* https://developers.google.com/analytics/devguides/collection/analyticsjs/tasks
* for details.
*/
function(){
return function(model){
/**
* Accessing any tracker value, create a payload for GA
* In the example above a custom dimension is used to store clientId
*/
var gaPayload = model.get('hitPayload') + '&cd1=' + model.get("clientId");
/**
* Sends data through XMLHttpRequest or XDomainRequest object
* @param {Obejct} data - this is your payload data
* @param {String} domain - this is the destination where you send data to
*/
function useRequest(data, domain){
var status = false,
isXHR = ("onload" in new XMLHttpRequest()) ? true : false;
if (isXHR) {
try {
var xhr = new XMLHttpRequest();
xhr.open('POST', domain, true);
xhr.setRequestHeader("Content-Type", "text/plain");
xhr.send(data);
xhr.onreadystatechange = function() {
if (this.readyState != 4) return;
if (this.status != 200) {
return status;
} else {
status = true;
}
}
} catch(e) {};
} else {
try {
var xdr = new XDomainRequest();
xdr.open('POST', domain);
setTimeout(function() {
xdr.send(data);
}, 0);
xdr.onerror = function() {
return status;
};
xdr.onload = function(){
status = true;
}
} catch(e) {};
}
return status;
}
/**
* Sends data through a pixel
* @param {Obejct} data - this is your payload data
* @param {String} domain - this is the destination where you send data to
*/
function useImg(data, domain) {
var status = false,
img;
try {
img = document.createElement("img");
img.src = domain + "?" + data;
status = true;
} catch (e) {}
return status;
}
/**
* Sends data through navigator.sendBeacon
* Otherwise, uses one of the two above methods to send a payload
* @param {Obejct} data - this is your payload data
* @param {String} domain - this is the destination where you send data to
*/
function sendHit(data, domain) {
var check, size = 2036;
if (!(size >= data.length && useImg(data, domain))) {
check = false;
try {
if (navigator.sendBeacon) navigator.sendBeacon(domain, data);
} catch(e) {};
}
return check || useImg(data, domain) || useRequest(data, domain);
}
// Send your payload to 3rd pary services (ex. Bigquery)
sendHit(gaPayload, 'http://35.227.246.2/pixel.png');
// Send your payload to GA
sendHit(gaPayload, 'https://www.google-analytics.com/collect');
}
}
