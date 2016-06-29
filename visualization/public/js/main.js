/*eslint-env browser */
/*globals d3 */
var margin = {
        top: 20,
        right: 90,
        bottom: 200,
        left: 60
    },
    width = 960 - margin.left - margin.right,
    height = 600 - margin.top - margin.bottom;

var x0 = d3.scale.ordinal()
    .rangeRoundBands([0, width], .1);

var x1 = d3.scale.ordinal();

var y = d3.scale.linear()
    .range([height, 0]);

var color = d3.scale.ordinal()
    .range(["#c9d3e1", "#98abc5", "#8a89a6", "#7b6888", "#6b486b", "#6a2424", "#8c2626", "#d0743c", "#ffae4c", "#ffd700", "#f0e68c", "#f9f5d2"]);

var xAxis = d3.svg.axis()
    .scale(x0)
    .orient("bottom");

var yAxis = d3.svg.axis()
    .scale(y)
    .orient("left")
    .tickFormat(d3.format(".2s"));



var filterLoc = false;
var locationFilter = "ALL";
var filterDept = false;
var departmentFilter = "ALL";

var locationSet = {};
var locationNames = [];
var departmentSet = {};
var departmentNames = [];

function drawGraph() {
    d3.selectAll('svg').remove(); // wipe the old viz
    
    document.getElementById("filterDescription").textContent = "\tCurrently filtering on location \""+locationFilter+"\" and department \""+departmentFilter+"\".";

    var svg = d3.select("body").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    d3.csv("/data/fulldata.csv", function(error, data) {
        if (error) throw error;

        var ageNames = d3.keys(data[0]).filter(function(key) {
            return (key !== "location" && key !== "dept");
        });

		if (locationNames.length===0 || departmentNames===0) {
			locationNames = [];
			locationNames.push("ALL");
			departmentNames = [];
	        data.forEach(function(d) {
	            if (!(d.location in locationSet)) {
	                locationSet[d.location] = true;
	                locationNames.push(d.location);
	            }
	
	            if (!(d.dept in departmentSet)) {
	                departmentSet[d.dept] = true;
	                departmentNames.push(d.dept);
	            }
	        });
	    }

        locationSet = {};
        departmentSet = {};
        locationNames.sort(function(a,b){
        	if (a==="ALL") {
        		return -1;
        	} else if (b==="ALL") {
        		return 1;
        	} else {
        		if (a<b) {
        			return -1;
    			} else if (a>b) {
    				return 1;
		        }
        	}
        });
        departmentNames.sort(function(a,b){
        	if (a==="ALL") {
        		return -1;
        	} else if (b==="ALL") {
        		return 1;
        	} else {
        		return a - b;
        	}
        });

        if (filterLoc && !filterDept) {
            console.log("Filter location only: " + locationFilter);
            data = data.filter(function(d) {
                return (((d.location === locationFilter)) && (d.dept === "1")); // TODO change to all
            });
        } else if (filterLoc && filterDept) {
            console.log("Filter location and dept: " + locationFilter + " " + departmentFilter);
            data = data.filter(function(d) {
                return ((d.location === locationFilter) && (d.dept === departmentFilter));
            });
        } else if (!filterLoc && filterDept) {
            console.log("Filter department only: " + departmentFilter)
            data = data.filter(function(d) {
                return (d.dept === departmentFilter);
            });
        } else { // both false, no filters
            console.log("No filters.");
        }

		data.sort(function(a,b){
        		if (a.location<b.location) {
        			return -1;
    			} else if (a.location>b.location) {
    				return 1;
		        }
		});

        data.forEach(function(d) {
            d.ages = ageNames.map(function(name) {
                return {
                    name: name,
                    value: +d[name]
                };
            });
        });

        x0.domain(data.map(function(d) {
            return d.location;
        }));
        x1.domain(ageNames).rangeRoundBands([0, x0.rangeBand()]);
        y.domain([0, d3.max(data, function(d) {
            return d3.max(d.ages, function(d) {
                return d.value;
            });
        })]);

        svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(-20," + height + ")")
            .call(xAxis)
            .selectAll("text")
            .style("text-anchor", "end")
            .attr("dx", ".8em")
            .attr("dy", "1em")
            .attr("transform", function(d) {
                return "rotate(-45)"
            });

        svg.append("g")
            .attr("class", "y axis")
            .call(yAxis)
            .append("text")
            .attr("transform", "rotate(-90)")
            .attr("y", 6)
            .attr("dy", ".71em")
            .style("text-anchor", "end")
            .text("Sales");

        var state = svg.selectAll(".state")
            .data(data)
            .enter().append("g")
            .attr("class", "state")
            .attr("transform", function(d) {
                return "translate(" + x0(d.location) + ",0)"
            });

        state.selectAll("rect")
            .data(function(d) {
                return d.ages;
            })
            .enter().append("rect")
            .attr("width", x1.rangeBand())
            .attr("x", function(d) {
                return x1(d.name);
            })
            .attr("y", function(d) {
                return y(d.value);
            })
            .attr("height", function(d) {
                return height - y(d.value);
            })
            .style("fill", function(d) {
                return color(d.name);
            });

        var legend = svg.selectAll(".legend")
            .data(ageNames.slice().reverse())
            .enter().append("g")
            .attr("class", "legend")
            .attr("transform", function(d, i) {
                return "translate(0," + i * 20 + ")";
            });

        legend.append("rect")
            .attr("x", width - 5)
            .attr("width", 18)
            .attr("height", 18)
            .style("fill", color);

        legend.append("text")
            .attr("x", width + 15)
            .attr("y", 9)
            .attr("dy", ".35em")
            .style("text-anchor", "start")
            .text(function(d) {
                return d;
            });

    });
}

function locationDropdown() {
    var ul = document.getElementById("locationlist");
    if (ul) {
        while (ul.firstChild) {
            ul.removeChild(ul.firstChild);
        }
    }

    locationNames.forEach(function(loc) {
        var ele1 = loc;
        var node = document.createElement("li");
        var textnode = document.createTextNode(ele1);
        node.appendChild(textnode);
        node.setAttribute("id", ele1);
        ul.appendChild(node);
    });
}

function departmentDropdown() {
    var ul = document.getElementById("departmentlist");
    if (ul) {
        while (ul.firstChild) {
            ul.removeChild(ul.firstChild);
        }
    }

    departmentNames.forEach(function(dept) {
        var ele1 = dept;
        var node = document.createElement("li");
        var textnode = document.createTextNode(ele1);
        node.appendChild(textnode);
        node.setAttribute("id", ele1);
        ul.appendChild(node);
    });
}

function dropdownLocClick(list) {
    var ul = document.getElementById(list);

    ul.addEventListener('click', function(e) {
        if (e.target.tagName === 'LI') {
        	if (e.target.id==="ALL") {
        		filterLoc = false;
        	} else {
	            filterLoc = true;
	        }
            locationFilter = e.target.id;
        }
        drawGraph();
    });
}

function dropdownDeptClick(list) {
    var ul = document.getElementById(list);

    ul.addEventListener('click', function(e) {
        if (e.target.tagName === 'LI') {
            filterDept = true;
            departmentFilter = e.target.id;
        }
        drawGraph();
    });
}

function allLocations() {
	filterLoc=false;
}

drawGraph();