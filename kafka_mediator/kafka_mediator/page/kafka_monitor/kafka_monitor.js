frappe.pages["kafka-monitor"].on_page_load = function (wrapper) {
    var page = frappe.ui.make_app_page({
        parent: wrapper,
        title: "Kafka Monitor",
        single_column: true,
    });

    page.main.html(frappe.render_template("kafka_monitor", {}));
    KafkaMonitor.init(wrapper);
};

var KafkaMonitor = {
    cluster: null,
    countdown: 60,
    sites_countdown: 300,
    countdown_timer: null,
    selected_topic: null,
    bar_chart: null,
    lag_chart: null,
    last_data: null,
    _topic_page: 0,
    _topic_page_size: 5,
    _topic_total: 0,
    _topic_search: "",
    _all_topics_cache: [],

    init: function (wrapper) {
        this.wrapper = wrapper;
        // Scope all selectors to the page wrapper
        this.$ = function (sel) { return $(wrapper).find(sel); };
        this._inject_expand_buttons();
        this.load_clusters();
        this.load_sites();
        this.bind_events();
    },

    // ── Expand / collapse buttons ─────────────────────────────────────────────
    _inject_expand_buttons: function () {
        var expand_svg = function (id) {
            return (
                '<button class="km-btn km-btn-icon km-expand-btn" data-target="' + id + '" title="Expand / Collapse">' +
                '<svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none"' +
                ' stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">' +
                '<polyline points="15 3 21 3 21 9"></polyline><polyline points="9 21 3 21 3 15"></polyline>' +
                '<line x1="21" y1="3" x2="14" y2="10"></line><line x1="3" y1="21" x2="10" y2="14"></line>' +
                '</svg></button>'
            );
        };
        $(".km-chart-card .km-card-header, .km-table-card .km-card-header").each(function () {
            var parent_id = $(this).closest("[id]").attr("id") ||
                            $(this).closest(".km-chart-card, .km-table-card").attr("id");
            if (!parent_id) {
                parent_id = "km-card-" + Math.random().toString(36).slice(2, 7);
                $(this).closest(".km-chart-card, .km-table-card").attr("id", parent_id);
            }
            $(this).append(expand_svg(parent_id));
        });
    },

    _toggle_expand: function (target_id) {
        var $el = $("#" + target_id);
        var is_expanded = $el.hasClass("km-card-expanded");
        // Collapse all first
        $(".km-card-expanded").each(function () {
            $(this).removeClass("km-card-expanded");
            $(this).find(".km-expand-btn svg").html(
                '<polyline points="15 3 21 3 21 9"></polyline><polyline points="9 21 3 21 3 15"></polyline>' +
                '<line x1="21" y1="3" x2="14" y2="10"></line><line x1="3" y1="21" x2="10" y2="14"></line>'
            );
        });
        if (!is_expanded) {
            $el.addClass("km-card-expanded");
            $el.find(".km-expand-btn svg").html(
                '<polyline points="4 14 10 14 10 20"></polyline><polyline points="20 10 14 10 14 4"></polyline>' +
                '<line x1="10" y1="14" x2="3" y2="21"></line><line x1="21" y1="3" x2="14" y2="10"></line>'
            );
        }
    },

    // ── Events ────────────────────────────────────────────────────────────────
    bind_events: function () {
        var self = this;

        $(this.wrapper).on("change", "#km-cluster-select", function () {
            self.cluster = $(this).val();
            self.selected_topic = null;
            self.close_panel();
            if (self.cluster) { self.refresh(); self.reset_countdown(); }
        });

        $(this.wrapper).on("click", "#km-refresh-btn", function () {
            self.reset_countdown();
            self.refresh();
        });

        $(this.wrapper).on("click", "#km-sync-btn", function () {
            self._do_sync();
        });

        $(this.wrapper).on("click", "#km-panel-close", function () {
            self.close_panel();
        });

        $(this.wrapper).on("click", ".km-topic-row", function () {
            var topic = $(this).data("topic");
            self.open_panel(topic);
        });

        $(this.wrapper).on("click", "#km-add-site-btn", function () {
            self._do_add_site();
        });

        $(this.wrapper).on("click", "#km-sites-refresh-btn", function () {
            self.sites_countdown = 300;
            self.check_sites();
        });

        $(this.wrapper).on("click", ".km-site-toggle-btn", function (e) {
            e.stopPropagation();
            self._do_toggle_site($(this).data("name"));
        });

        $(this.wrapper).on("click", ".km-site-delete-btn", function (e) {
            e.stopPropagation();
            self._do_delete_site($(this).data("name"), $(this).data("site"));
        });

        $(this.wrapper).on("click", ".km-expand-btn", function (e) {
            e.stopPropagation();
            self._toggle_expand($(this).data("target"));
        });

        $(this.wrapper).on("click", "#km-topics-load-more-btn", function () {
            self.load_more_topics();
        });

        var _km_search_timer = null;
        $(this.wrapper).on("input", "#km-topic-search", function () {
            var val = $(this).val().trim();
            clearTimeout(_km_search_timer);
            _km_search_timer = setTimeout(function () {
                self._topic_search = val;
                self._topic_page = 0;
                if (self.last_data) self.render(self.last_data);
            }, 300);
        });

        $(this.wrapper).on("click", ".km-retention-btn", function (e) {
            e.stopPropagation();
            var $btn = $(this);
            self._do_set_retention($btn.data("topic"), $btn.data("current"));
        });

        $(this.wrapper).on("click", ".km-scale-btn", function (e) {
            e.stopPropagation();
            var $btn = $(this);
            self._do_scale($btn.data("topic"), $btn.data("group"), parseInt($btn.data("current"), 10));
        });

        $(this.wrapper).on("click", ".km-restart-btn", function (e) {
            e.stopPropagation();
            var $btn = $(this);
            self._do_restart_group($btn.data("topic"), $btn.data("group"), $btn);
        });
    },

    // ── Cluster loading ───────────────────────────────────────────────────────
    load_clusters: function () {
        var self = this;
        frappe.call({
            method: "frappe.client.get_list",
            args: { doctype: "Kafka Cluster", fields: ["name"], limit: 50 },
            callback: function (r) {
                var clusters = r.message || [];
                var $sel = self.$("#km-cluster-select");
                $sel.empty();
                if (!clusters.length) {
                    $sel.append('<option value="">No clusters found</option>');
                    return;
                }
                $sel.append('<option value="">— Select cluster —</option>');
                clusters.forEach(function (c) {
                    $sel.append('<option value="' + c.name + '">' + c.name + '</option>');
                });
                self.cluster = clusters[0].name;
                $sel.val(self.cluster);
                self.refresh();
                self.start_countdown();
            }
        });
    },

    // ── Refresh ───────────────────────────────────────────────────────────────
    refresh: function () {
        if (!this.cluster) return;
        var self = this;
        self.$("#km-refresh-icon").addClass("km-spinning");
        self._topic_page = 0;

        frappe.call({
            method: "kafka_mediator.api.topic.get_cluster_stats",
            args: { cluster_name: self.cluster },
            error: function (r) {
                self.$("#km-refresh-icon").removeClass("km-spinning");
                var msg = (r && r.message) ? r.message : "Server error — check Error Log";
                frappe.show_alert({ message: "Stats error: " + msg, indicator: "red" });
            },
            callback: function (r) {
                self.$("#km-refresh-icon").removeClass("km-spinning");
                if (!r || r.status_code !== 200) {
                    frappe.show_alert({ message: (r && r.message) || "Failed to load stats", indicator: "orange" });
                    return;
                }
                self.last_data = r.data;
                self.render(r.data);
                self._auto_restart_lagging(r.data);
                self._update_footer();
                if (self.selected_topic) {
                    var updated = (r.data.topics || []).find(function (t) {
                        return t.topic_name === self.selected_topic;
                    });
                    if (updated) self.render_panel(updated);
                }
            }
        });
    },

    // ── Auto-restart groups with lag and no live members (after 5 min) ───────
    _lag_detected_at: {},   // key → timestamp (ms) when bad state was first seen
    _restarting: {},        // key → bool, in-flight guard

    _auto_restart_lagging: function (data) {
        var self = this;
        var now = Date.now();
        var DELAY_MS = 5 * 60 * 1000;
        var seen_keys = {};

        (data.topics || []).forEach(function (t) {
            (t.consumer_groups || []).forEach(function (g) {
                if ((g.total_lag || 0) > 0 && (g.member_count || 0) === 0) {
                    var key = t.topic_name + "|" + (g.consumer_group || "");
                    seen_keys[key] = true;

                    if (!self._lag_detected_at[key]) {
                        self._lag_detected_at[key] = now;   // start the clock
                    }

                    var elapsed = now - self._lag_detected_at[key];
                    if (elapsed < DELAY_MS || self._restarting[key]) return;

                    self._restarting[key] = true;
                    frappe.call({
                        method: "kafka_mediator.api.consumer.restart_consumer_group",
                        args: { topic_name: t.topic_name, consumer_group: g.consumer_group },
                        callback: function (r) {
                            delete self._restarting[key];
                            delete self._lag_detected_at[key];  // reset clock after restart
                            if (r && r.status_code === 200) {
                                frappe.show_alert({ message: "Auto-restarted: " + (g.consumer_group || "") + " on " + t.topic_name, indicator: "orange" });
                            }
                        },
                        error: function () {
                            delete self._restarting[key];
                        }
                    });
                }
            });
        });

        // Clear clock for groups that recovered on their own
        Object.keys(self._lag_detected_at).forEach(function (key) {
            if (!seen_keys[key]) delete self._lag_detected_at[key];
        });
    },

    // ── Render KPIs + table + charts ─────────────────────────────────────────
    render: function (data) {
        var all_topics = data.topics || [];
        this._all_topics_cache = all_topics;

        // Filter by search
        var search = this._topic_search.toLowerCase();
        if (search) {
            all_topics = all_topics.filter(function (t) {
                return (t.topic_name || "").toLowerCase().indexOf(search) !== -1;
            });
        }
        this._topic_total = all_topics.length;
        this._filtered_topics = all_topics;

        // KPIs
        this.$("#km-kpi-topics").text(data.total_topics || 0);
        this.$("#km-kpi-messages").text(this.fmt_num(data.total_messages || 0));

        var lag = data.total_lag || 0;
        this.$("#km-kpi-lag").text(lag);
        this.$("#km-kpi-lag-card").toggleClass("km-lag-lag-alert", lag > 0);

        this.$("#km-kpi-consumers").text(data.total_consumers || 0);
        this.$("#km-kpi-status").html(
            '<span class="km-status-pill km-pill-green"><span class="km-dot km-dot-green"></span>' +
            (data.broker_status || "Connected") + '</span>'
        );

        // Badge
        this.$("#km-topic-count").text(this._topic_total);

        // Table — first page
        var topics = all_topics.slice(0, this._topic_page_size);
        this._render_topic_table(topics);
        this._update_load_more();

        // Charts always use all topics (unfiltered)
        this._render_charts(data.topics || []);
    },

    _render_topic_table: function (topics) {
        var $tbody = this.$("#km-topic-tbody").empty();
        if (!topics.length) {
            $tbody.html('<tr><td colspan="7" class="km-empty">No topics in this cluster.</td></tr>');
        } else {
            topics.forEach(function (t) {
                var pill = KafkaMonitor._status_pill(t.consumer_groups || []);
                var lag_class = t.lag > 0 ? "km-lag-warn" : "km-lag-ok";
                var active_row = (t.topic_name === KafkaMonitor.selected_topic) ? " km-row-active" : "";
                var group_count = (t.consumer_groups || []).length;
                var retention_label = (t.retention_days === null || t.retention_days === undefined)
                    ? '—'
                    : (t.retention_days < 0 ? '∞' : t.retention_days + 'd');
                $tbody.append(
                    '<tr class="km-topic-row' + active_row + '" data-topic="' + t.topic_name + '">' +
                    '<td class="km-topic-name" data-full="' + t.topic_name + '">' + t.topic_name + '</td>' +
                    '<td class="text-right km-num">' + (t.partitions || 0) + '</td>' +
                    '<td class="text-right km-num">' + KafkaMonitor.fmt_num(t.message_count || 0) + '</td>' +
                    '<td class="text-right km-num ' + lag_class + '">' + (t.lag || 0) + '</td>' +
                    '<td class="text-right km-num">' + group_count + '</td>' +
                    '<td class="text-right km-num">' + retention_label + '</td>' +
                    '<td>' + pill + '</td>' +
                    '</tr>'
                );
            });
        }
    },

    _update_load_more: function () {
        var $wrap = this.$("#km-topics-load-more");
        var $btn = this.$("#km-topics-load-more-btn");
        var loaded = Math.min(this._topic_page * this._topic_page_size + this._topic_page_size, this._topic_total);
        if (loaded >= this._topic_total || !this._topic_total) {
            $wrap.hide();
        } else {
            $btn.text("Load More (" + loaded + "/" + this._topic_total + ")");
            $wrap.show();
        }
    },

    load_more_topics: function () {
        this._topic_page++;
        var all = this._filtered_topics || [];
        var topics = all.slice(0, this._topic_page * this._topic_page_size + this._topic_page_size);
        this._render_topic_table(topics);
        this._update_load_more();
    },

    _status_pill: function (consumer_groups) {
        if (!consumer_groups || !consumer_groups.length)
            return '<span class="km-status-pill km-pill-grey"><span class="km-dot km-dot-grey"></span>no groups</span>';
        var statuses = consumer_groups.map(function (g) { return g.thread_status || "stopped"; });
        var s, cls;
        if (statuses.every(function (x) { return x === "running"; })) { s = "running"; cls = "km-pill-green km-dot-green"; }
        else if (statuses.some(function (x) { return x === "error"; })) { s = "error"; cls = "km-pill-red km-dot-red"; }
        else if (statuses.some(function (x) { return x === "restarting"; })) { s = "restarting"; cls = "km-pill-orange km-dot-orange"; }
        else { s = "stopped"; cls = "km-pill-grey km-dot-grey"; }
        var dot_cls = cls.split(" ")[1];
        var pill_cls = cls.split(" ")[0];
        return '<span class="km-status-pill ' + pill_cls + '"><span class="km-dot ' + dot_cls + '"></span>' + s + '</span>';
    },

    // Cache the label set so we only rebuild a chart when the topics actually
    // change; otherwise we update values in place — this stops the grow-from-zero
    // animation (and the skewed-label flicker) from replaying on every refresh.
    _chart_labels_key: null,

    _render_charts: function (topics) {
        if (!topics.length) return;
        var labels = topics.map(function (t) {
            return t.topic_name.length > 14 ? t.topic_name.slice(0, 12) + "…" : t.topic_name;
        });
        var full_names = topics.map(function (t) { return t.topic_name; });
        var msg_values = topics.map(function (t) { return t.message_count || 0; });
        var lag_values = topics.map(function (t) { return t.lag || 0; });
        var labels_key = labels.join("|");
        var labels_changed = labels_key !== this._chart_labels_key;

        // Map a (possibly truncated) label back to its full topic name for tooltips.
        var full_by_label = {};
        labels.forEach(function (l, i) { full_by_label[l] = full_names[i]; });
        var tipX = function (d) { return full_by_label[d] || d; };

        var bar_el = this.$("#km-bar-chart")[0];
        if (bar_el) {
            if (this.bar_chart && !labels_changed) {
                try { this.bar_chart.update({ labels: labels, datasets: [{ name: "Messages", values: msg_values, chartType: "bar" }] }); }
                catch (e) { this.bar_chart = null; }
            }
            if (!this.bar_chart) {
                bar_el.innerHTML = "";
                try {
                    this.bar_chart = new frappe.Chart(bar_el, {
                        type: "bar",
                        height: 170,
                        data: { labels: labels, datasets: [{ name: "Messages", values: msg_values, chartType: "bar" }] },
                        colors: ["#4f46e5"],
                        axisOptions: { xAxisMode: "tick", xIsSeries: true },
                        barOptions: { spaceRatio: 0.5 },
                        tooltipOptions: { formatTooltipX: tipX, formatTooltipY: function (d) { return KafkaMonitor.fmt_num(d); } }
                    });
                } catch (e) { bar_el.innerHTML = '<p style="padding:20px;color:#94a3b8;font-size:12px">Chart unavailable</p>'; }
            }
        }

        var lag_el = this.$("#km-lag-chart")[0];
        if (lag_el) {
            if (this.lag_chart && !labels_changed) {
                try { this.lag_chart.update({ labels: labels, datasets: [{ name: "Lag", values: lag_values, chartType: "bar" }] }); }
                catch (e) { this.lag_chart = null; }
            }
            if (!this.lag_chart) {
                lag_el.innerHTML = "";
                try {
                    this.lag_chart = new frappe.Chart(lag_el, {
                        type: "bar",
                        height: 170,
                        data: { labels: labels, datasets: [{ name: "Lag", values: lag_values, chartType: "bar" }] },
                        colors: ["#ea580c"],
                        axisOptions: { xAxisMode: "tick", xIsSeries: true },
                        barOptions: { spaceRatio: 0.5 },
                        tooltipOptions: { formatTooltipX: tipX, formatTooltipY: function (d) { return d; } }
                    });
                } catch (e) { lag_el.innerHTML = '<p style="padding:20px;color:#94a3b8;font-size:12px">Chart unavailable</p>'; }
            }
        }

        this._chart_labels_key = labels_key;
    },

    // ── Side Panel ────────────────────────────────────────────────────────────
    open_panel: function (topic_name) {
        var self = this;
        this.selected_topic = topic_name;
        self.$("#km-panel").addClass("km-panel-open");
        self.$("#km-table-wrap").addClass("km-table-narrow");
        self.$("#km-panel-title").text(topic_name);
        self.$(".km-topic-row").removeClass("km-row-active");
        self.$(".km-topic-row[data-topic='" + topic_name + "']").addClass("km-row-active");
        self.$("#km-panel-body").html('<p class="km-placeholder">Loading…</p>');

        frappe.call({
            method: "kafka_mediator.api.topic.get_topic_stats",
            args: { topic_name: topic_name },
            callback: function (r) {
                if (!r || r.status_code !== 200) {
                    self.$("#km-panel-body").html('<p class="km-placeholder" style="color:#ef4444">Failed to load topic stats.</p>');
                    return;
                }
                self.render_panel(r.data);
            }
        });
    },

    render_panel: function (data) {
        var consumer_groups = data.consumer_groups || [];
        var lag_class = (data.lag || 0) > 0 ? "km-lag-warn" : "";
        var html = "";

        var retention_label = (data.retention_days === null || data.retention_days === undefined)
            ? "—"
            : (data.retention_days < 0 ? "∞ infinite" : data.retention_days + "d");

        html += '<div class="km-pstat-grid">';
        html += '<div class="km-pstat"><div class="km-pstat-label">Partitions</div><div class="km-pstat-value">' + (data.partitions || 0) + '</div></div>';
        html += '<div class="km-pstat"><div class="km-pstat-label">Messages</div><div class="km-pstat-value">' + this.fmt_num(data.message_count || 0) + '</div></div>';
        html += '<div class="km-pstat"><div class="km-pstat-label">Total Lag</div><div class="km-pstat-value ' + lag_class + '">' + (data.lag || 0) + '</div></div>';
        html += '<div class="km-pstat"><div class="km-pstat-label">Replication</div><div class="km-pstat-value">' + (data.replication_factor || 0) + '</div></div>';
        html += '<div class="km-pstat km-pstat-retention" style="grid-column:1/-1;display:flex;align-items:center;justify-content:space-between;">';
        html += '<div><div class="km-pstat-label">Retention</div><div class="km-pstat-value" style="font-size:18px">' + retention_label + '</div></div>';
        html += '<button class="km-btn km-retention-btn" style="font-size:11px;padding:4px 12px;height:28px" data-topic="' + data.topic_name + '" data-current="' + (data.retention_days !== null && data.retention_days !== undefined ? data.retention_days : "") + '">Set</button>';
        html += '</div>';
        html += '</div>';

        html += '<div class="km-section-title">Consumer Groups (' + consumer_groups.length + ')</div>';

        if (!consumer_groups.length) {
            html += '<p class="km-placeholder" style="margin-top:12px;text-align:left">No consumer groups on this topic.</p>';
        } else {
            consumer_groups.forEach(function (g) {
                var group_name  = g.consumer_group || "—";
                var member_count = g.member_count || 0;   // live members in Kafka
                var frappe_list = g.frappe_consumers || [];
                var st = g.thread_status || "stopped";
                var dot_cls  = { running: "km-dot-green",  error: "km-dot-red",  restarting: "km-dot-orange"  }[st] || "km-dot-grey";
                var pill_cls = { running: "km-pill-green", error: "km-pill-red", restarting: "km-pill-orange" }[st] || "km-pill-grey";
                var group_lag = g.total_lag || 0;

                var needs_restart = (group_lag > 0 && member_count === 0);

                html += '<div class="km-group-header">';
                html += '<span class="km-group-label">' + group_name + '</span>';
                if (needs_restart) {
                    html += '<span style="background:#fef3c7;color:#92400e;border:1px solid #fbbf24;'
                         +  'border-radius:4px;padding:2px 8px;font-size:11px;font-weight:600;margin-right:6px">'
                         +  '⚠ No live members</span>';
                }
                html += '<span class="km-group-count">' + member_count + ' live member(s)</span>';
                if (needs_restart) {
                    html += '<button class="km-btn km-restart-btn"'
                         +  ' style="background:#f97316;color:#fff;border-color:#ea580c;margin-right:4px"'
                         +  ' data-topic="' + data.topic_name + '"'
                         +  ' data-group="' + group_name + '">'
                         +  'Restart</button>';
                }
                html += '<button class="km-btn km-scale-btn"'
                     +  ' data-topic="' + data.topic_name + '"'
                     +  ' data-group="' + group_name + '"'
                     +  ' data-current="' + frappe_list.length + '">'
                     +  'Scale</button>';
                html += '</div>';

                // Group summary card
                html += '<div class="km-consumer-card">';
                html += '<div class="km-consumer-header">';
                html += '<span class="km-dot ' + dot_cls + '"></span>';
                html += '<span class="km-consumer-name">' + group_name + '</span>';
                html += '<span class="km-status-pill ' + pill_cls + '">' + st + '</span>';
                html += '</div>';
                html += '<div class="km-consumer-meta">';
                html += '<div class="km-meta-row"><span class="km-meta-label">Lag</span>'
                     +  '<span class="km-meta-value ' + (group_lag > 0 ? "km-lag-warn" : "km-lag-ok") + '">' + group_lag + '</span></div>';
                html += '<div class="km-meta-row"><span class="km-meta-label">Live Members</span>'
                     +  '<span class="km-meta-value">' + member_count + '</span></div>';
                html += '<div class="km-meta-row"><span class="km-meta-label">Frappe Docs</span>'
                     +  '<span class="km-meta-value">' + frappe_list.length + '</span></div>';
                html += '</div>';

                // Frappe consumer docs (thread tracking rows)
                if (frappe_list.length) {
                    frappe_list.forEach(function (c) {
                        var cst = c.thread_status || "stopped";
                        var cdot  = { running: "km-dot-green",  error: "km-dot-red",  restarting: "km-dot-orange"  }[cst] || "km-dot-grey";
                        var cpill = { running: "km-pill-green", error: "km-pill-red", restarting: "km-pill-orange" }[cst] || "km-pill-grey";
                        var heartbeat = c.last_heartbeat ? String(c.last_heartbeat).split(".")[0] : "—";
                        html += '<div style="padding:6px 12px 6px 24px;border-top:1px solid var(--border-color,#f1f5f9);display:flex;align-items:center;gap:8px;font-size:12px;">';
                        html += '<span class="km-dot ' + cdot + '"></span>';
                        html += '<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#374151">' + c.name + '</span>';
                        html += '<span class="km-status-pill ' + cpill + '" style="font-size:10px">' + cst + '</span>';
                        html += '<span style="color:#94a3b8;font-size:11px">' + heartbeat + '</span>';
                        html += '</div>';
                    });
                }

                html += '</div>';  // end km-consumer-card
            });
        }

        this.$("#km-panel-body").html(html);
    },

    _do_sync: function () {
        var self = this;
        var $btn = self.$("#km-sync-btn");
        $btn.prop("disabled", true).text("Syncing…");

        frappe.call({
            method: "kafka_mediator.api.topic.sync_with_kafka",
            args: {},
            callback: function (r) {
                $btn.prop("disabled", false).html(
                    '<svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none"' +
                    ' stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">' +
                    '<path d="M21 2v6h-6"></path><path d="M3 12a9 9 0 0 1 15-6.7L21 8"></path>' +
                    '<path d="M3 22v-6h6"></path><path d="M21 12a9 9 0 0 1-15 6.7L3 16"></path>' +
                    '</svg> Sync with Kafka'
                );

                if (!r || r.status_code !== 200) {
                    frappe.show_alert({ message: (r && r.message) || "Sync failed.", indicator: "red" });
                    return;
                }

                var d = r.data || {};
                var parts = [];
                if (d.recreated_topics && d.recreated_topics.length)
                    parts.push(d.recreated_topics.length + " topic(s) recreated");
                if (d.restarted_consumers && d.restarted_consumers.length)
                    parts.push(d.restarted_consumers.length + " consumer(s) restarted");
                if (d.failed_topics && d.failed_topics.length)
                    parts.push(d.failed_topics.length + " topic(s) failed");
                if (d.failed_consumers && d.failed_consumers.length)
                    parts.push(d.failed_consumers.length + " consumer(s) failed");

                var indicator = (d.failed_topics && d.failed_topics.length) || (d.failed_consumers && d.failed_consumers.length)
                    ? "orange" : "green";
                var summary = parts.length ? parts.join(", ") : "Everything in sync";
                frappe.show_alert({ message: summary, indicator: indicator });
                self.refresh();
            },
            error: function () {
                $btn.prop("disabled", false).text("Sync with Kafka");
                frappe.show_alert({ message: "Sync request failed.", indicator: "red" });
            }
        });
    },

    _do_scale: function (topic_name, consumer_group, current_count) {
        var self = this;

        frappe.prompt(
            [{
                fieldname: "target_count",
                label: "Target consumer count",
                fieldtype: "Int",
                default: current_count + 1,
                description: "Currently " + current_count + " active. Enter a number to scale up or down.",
                reqd: 1
            }],
            function (values) {
                var target = parseInt(values.target_count, 10);
                if (!target || target < 1) {
                    frappe.show_alert({ message: "Enter a valid target count.", indicator: "red" });
                    return;
                }
                frappe.call({
                    method: "kafka_mediator.api.consumer.scale_consumers",
                    args: {
                        topic_name:     topic_name,
                        consumer_group: consumer_group,
                        target_count:   target
                    },
                    callback: function (r) {
                        if (!r || r.status_code !== 200) {
                            frappe.show_alert({ message: (r && r.message) || "Scale failed.", indicator: "red" });
                            return;
                        }
                        frappe.show_alert({ message: r.message, indicator: "green" });
                        self.refresh();
                        if (self.selected_topic === topic_name) {
                            self.open_panel(topic_name);
                        }
                    }
                });
            },
            "Scale Consumers — " + topic_name,
            "Scale"
        );
    },

    _do_set_retention: function (topic_name, current_days) {
        var self = this;
        var default_val = (current_days !== null && current_days !== undefined && current_days !== "") ? current_days : 7;

        frappe.prompt(
            [{
                fieldname: "retention_days",
                label: "Retention (days)",
                fieldtype: "Float",
                default: default_val,
                description: "Minimum 5 days. Enter -1 for infinite retention.",
                reqd: 1
            }],
            function (values) {
                var days = parseFloat(values.retention_days);
                if (isNaN(days) || (days !== -1 && days < 5)) {
                    frappe.show_alert({ message: "Minimum retention is 5 days (or -1 for infinite).", indicator: "red" });
                    return;
                }
                frappe.call({
                    method: "kafka_mediator.api.topic.set_topic_retention",
                    args: { topic_name: topic_name, retention_days: days },
                    callback: function (r) {
                        if (!r || r.status_code !== 200) {
                            frappe.show_alert({ message: (r && r.message) || "Failed to set retention.", indicator: "red" });
                            return;
                        }
                        frappe.show_alert({ message: r.message, indicator: "green" });
                        if (self.selected_topic === topic_name) {
                            self.open_panel(topic_name);
                        }
                    }
                });
            },
            "Set Retention — " + topic_name,
            "Apply"
        );
    },

    _do_restart_group: function (topic_name, consumer_group, $btn) {
        var self = this;
        $btn.prop("disabled", true).text("Restarting…");
        frappe.call({
            method: "kafka_mediator.api.consumer.restart_consumer_group",
            args: { topic_name: topic_name, consumer_group: consumer_group },
            callback: function (r) {
                $btn.prop("disabled", false).text("Restart");
                if (!r || r.status_code !== 200) {
                    frappe.show_alert({ message: (r && r.message) || "Restart failed.", indicator: "red" });
                    return;
                }
                frappe.show_alert({ message: r.message, indicator: "green" });
                self.refresh();
                if (self.selected_topic === topic_name) {
                    self.open_panel(topic_name);
                }
            },
            error: function () {
                $btn.prop("disabled", false).text("Restart");
                frappe.show_alert({ message: "Restart request failed.", indicator: "red" });
            }
        });
    },

    close_panel: function () {
        this.selected_topic = null;
        this.$("#km-panel").removeClass("km-panel-open");
        this.$("#km-table-wrap").removeClass("km-table-narrow");
        this.$(".km-topic-row").removeClass("km-row-active");
    },

    // ── Countdown ─────────────────────────────────────────────────────────────
    start_countdown: function () {
        var self = this;
        self.countdown = 10;
        clearInterval(self.countdown_timer);
        self.countdown_timer = setInterval(function () {
            self.countdown--;
            self.$("#km-countdown").text(self.countdown + "s");
            if (self.countdown <= 0) {
                self.countdown = 60;
                self.refresh();
            }

            self.sites_countdown--;
            var sm = Math.floor(self.sites_countdown / 60);
            var ss = self.sites_countdown % 60;
            self.$("#km-sites-countdown").text(sm + "m " + (ss < 10 ? "0" + ss : ss) + "s");
            if (self.sites_countdown <= 0) {
                self.sites_countdown = 300;
                self.check_sites();
            }
        }, 1000);
    },

    reset_countdown: function () {
        this.countdown = 60;
        clearInterval(this.countdown_timer);
        this.start_countdown();
    },

    // ── Footer timestamp ──────────────────────────────────────────────────────
    _update_footer: function () {
        var now = new Date();
        var pad = function (n) { return n < 10 ? "0" + n : n; };
        var ts = pad(now.getHours()) + ":" + pad(now.getMinutes()) + ":" + pad(now.getSeconds());
        this.$("#km-last-updated").text("Last updated: " + ts);
    },

    // ── Site Monitor ─────────────────────────────────────────────────────────
    load_sites: function () {
        var self = this;
        frappe.call({
            method: "kafka_mediator.api.site_monitor.list_sites",
            callback: function (r) {
                if (r && r.status_code === 200) self.render_sites(r.data || []);
            }
        });
    },

    check_sites: function () {
        var self = this;
        self.$("#km-sites-refresh-icon").addClass("km-spinning");
        frappe.call({
            method: "kafka_mediator.api.site_monitor.check_sites",
            callback: function (r) {
                self.$("#km-sites-refresh-icon").removeClass("km-spinning");
                if (r && r.status_code === 200) self.render_sites(r.data || []);
            },
            error: function () {
                self.$("#km-sites-refresh-icon").removeClass("km-spinning");
            }
        });
    },

    render_sites: function (sites) {
        var $tbody = this.$("#km-sites-tbody").empty();
        this.$("#km-sites-count").text(sites.length);
        if (!sites.length) {
            $tbody.html('<tr><td colspan="6" class="km-empty">No sites configured.</td></tr>');
            return;
        }
        sites.forEach(function (s) {
            var st = s.status || "Unknown";
            var dot_cls  = { Up: "km-dot-green", Down: "km-dot-red" }[st] || "km-dot-grey";
            var pill_cls = { Up: "km-pill-green", Down: "km-pill-red" }[st] || "km-pill-grey";
            var pill = '<span class="km-status-pill ' + pill_cls + '"><span class="km-dot ' + dot_cls + '"></span>' + st + '</span>';

            var resp_html = '—';
            if (s.response_ms !== null && s.response_ms !== undefined) {
                var resp_cls = s.response_ms < 500 ? "km-site-resp-fast" : "km-site-resp-slow";
                resp_html = '<span class="' + resp_cls + '">' + s.response_ms + ' ms</span>';
            } else {
                resp_html = '<span class="km-site-resp-na">—</span>';
            }

            var last_checked = s.last_checked ? String(s.last_checked).split(".")[0] : "—";
            var on_cls = s.enabled ? " km-toggle-on" : "";
            var toggle_label = s.enabled ? "On" : "Off";

            var trash_svg = '<svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none"' +
                ' stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">' +
                '<polyline points="3 6 5 6 21 6"></polyline>' +
                '<path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"></path>' +
                '<path d="M10 11v6"></path><path d="M14 11v6"></path>' +
                '<path d="M9 6V4h6v2"></path></svg>';

            $tbody.append(
                '<tr>' +
                '<td class="km-site-name">' + s.site_name + '</td>' +
                '<td><span class="km-site-url" title="' + s.url + '">' + s.url + '</span></td>' +
                '<td class="text-right">' + pill + '</td>' +
                '<td class="text-right km-num">' + resp_html + '</td>' +
                '<td class="text-right" style="font-size:11px;color:#94a3b8">' + last_checked + '</td>' +
                '<td class="text-right" style="white-space:nowrap;display:flex;align-items:center;justify-content:flex-end;gap:8px;padding:8px 16px">' +
                '<div class="km-toggle-wrap km-site-toggle-btn" title="Enable / Disable" data-name="' + s.name + '">' +
                '<div class="km-toggle-track' + on_cls + '"><div class="km-toggle-knob"></div></div>' +
                '<span class="km-toggle-label">' + toggle_label + '</span>' +
                '</div>' +
                '<button class="km-btn km-site-delete-btn" title="Remove site" data-name="' + s.name + '" data-site="' + s.site_name + '">' + trash_svg + '</button>' +
                '</td>' +
                '</tr>'
            );
        });
    },

    _do_add_site: function () {
        var self = this;
        frappe.prompt(
            [
                { fieldname: "site_name", label: "Site Name", fieldtype: "Data", reqd: 1 },
                { fieldname: "url", label: "URL", fieldtype: "Data", reqd: 1, description: "e.g. https://mysite.example.com" }
            ],
            function (values) {
                frappe.call({
                    method: "kafka_mediator.api.site_monitor.add_site",
                    args: { site_name: values.site_name, url: values.url },
                    callback: function (r) {
                        if (!r || r.status_code !== 200) {
                            frappe.show_alert({ message: (r && r.message) || "Failed to add site.", indicator: "red" });
                            return;
                        }
                        frappe.show_alert({ message: r.message, indicator: "green" });
                        self.check_sites();
                    }
                });
            },
            "Add Monitored Site",
            "Add"
        );
    },

    _do_toggle_site: function (name) {
        var self = this;
        frappe.call({
            method: "kafka_mediator.api.site_monitor.toggle_site",
            args: { name: name },
            callback: function (r) {
                if (!r || r.status_code !== 200) {
                    frappe.show_alert({ message: (r && r.message) || "Failed.", indicator: "red" });
                    return;
                }
                self.load_sites();
            }
        });
    },

    _do_delete_site: function (name, site_name) {
        var self = this;
        frappe.confirm(
            "Remove site <b>" + site_name + "</b> from monitoring?",
            function () {
                frappe.call({
                    method: "kafka_mediator.api.site_monitor.delete_site",
                    args: { name: name },
                    callback: function (r) {
                        if (!r || r.status_code !== 200) {
                            frappe.show_alert({ message: (r && r.message) || "Failed.", indicator: "red" });
                            return;
                        }
                        frappe.show_alert({ message: r.message, indicator: "green" });
                        self.load_sites();
                    }
                });
            }
        );
    },

    // ── Helpers ───────────────────────────────────────────────────────────────
    fmt_num: function (n) { return Number(n).toLocaleString(); },
};
