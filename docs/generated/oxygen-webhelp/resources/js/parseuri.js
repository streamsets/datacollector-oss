/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// FROM: https://github.com/get/parseuri/blob/master/index.js

/**
 * Parses an URI
 *
 * @author Steven Levithan <stevenlevithan.com> (MIT license)
 * @api private
 */

// Added support for "file:" protocol
var re = /^(?:(?![^:@]+:[^:@\/]*@)(http|https|ws|wss|file):\/\/)?((?:(([^:@]*)(?::([^:@]*))?)?@)?((?:[a-f0-9]{0,4}:){2,7}[a-f0-9]{0,4}|[^:\/?#]*)(?::(\d*))?)(((\/(?:[^?#](?![^?#\/]*\.[^?#\/.]+(?:[?#]|$)))*\/?)?([^?#\/]*))(?:\?([^#]*))?(?:#(.*))?)/;

var parts = [
	'source', 'protocol', 'authority', 'userInfo', 'user', 'password', 'host', 'port', 'relative', 'path', 'directory', 'file', 'query', 'anchor'
];

function parseUri(str) {
	str = str.toString();
	var src = str,
		b = str.indexOf('['),
		e = str.indexOf(']');

	if (b != -1 && e != -1) {
		str = str.substring(0, b) + str.substring(b, e).replace(/:/g, ';') + str.substring(e, str.length);
	}

	var m = re.exec(str || ''),
		uri = {},
		i = 14;

	while (i--) {
		uri[parts[i]] = m[i] || '';
	}

	if (b != -1 && e != -1) {
		uri.source = src;
		uri.host = uri.host.substring(1, uri.host.length - 1).replace(/;/g, ':');
		// Keep square brackets
		uri.authority = uri.authority.replace(/;/g, ':');
		uri.ipv6uri = true;
	}

	// Parse parameters
	var q = {
		name:   "queryKey",
		parser: /(?:^|&)([^&=]*)=?([^&]*)/g
	};

	uri[q.name] = {};
	uri[parts[12]].replace(q.parser, function ($0, $1, $2) {
		if ($1) uri[q.name][$1] = $2;
	});

	return uri;
};

