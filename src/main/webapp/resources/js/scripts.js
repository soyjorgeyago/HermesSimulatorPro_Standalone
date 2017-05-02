function locationLogChartExtender() {
    this.cfg.axes.xaxis = {
        showTicks: false,
        showTickMarks: false,
    };
    this.cfg.highlighter = {
        show: true,
        tooltipLocation: 'ne',
        tooltipContentEditor: function (str, seriesIndex, pointIndex, plot) {
            var d = new Date();
            var parts = str.split(",");
            var milliseconds = parts[0];
            var seconds = parseInt((milliseconds / 1000) % 60);
            var minutes = parseInt((milliseconds / (1000 * 60)) % 60);
            var hours = parseInt((milliseconds / (1000 * 60 * 60)) % 24) - (d.getTimezoneOffset() / 60);
            var fhours = (hours > 9) ? hours : ('0' + hours);
            var fminutes = (minutes > 9) ? minutes : ('0' + minutes);
            var fseconds = (seconds > 9) ? seconds : ('0' + seconds);
            return fhours + ":" + fminutes + ":" + fseconds + " -> " + parts[1];
        }
    };
    this.cfg.seriesDefaults = {
        markerOptions: {size: 1},
    };
}

function enableAllTheseDays(date) {
    var d = (date.getDate() > 9) ? date.getDate() : ('0' + date.getDate());
    var m = (date.getMonth() + 1 > 9) ? date.getMonth() + 1 : ('0' + (date.getMonth() + 1));
    var y = date.getFullYear();
    var formattedDate = d + '/' + m + '/' + y;

    for (i = 0; i < enabledDays.length; i++) {
        if ($.inArray(formattedDate, enabledDays) != -1) {
            return [true];
        }
    }

    return [false];
}