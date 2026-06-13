app_name = "kafka_mediator"
app_title = "Kafka Mediator"
app_publisher = "Tiberbu HealthNet Limited"
app_description = "Frappe app to connect Kafka and mediator creation of topics, message production and consumption"
app_email = "info@tiberbu.com"
app_license = "mit"

# Apps
# ------------------

# required_apps = []

# Each item in the list will be shown as an app in the apps page
# add_to_apps_screen = [
# 	{
# 		"name": "kafka_mediator",
# 		"logo": "/assets/kafka_mediator/logo.png",
# 		"title": "Kafka Mediator",
# 		"route": "/kafka_mediator",
# 		"has_permission": "kafka_mediator.api.permission.has_app_permission"
# 	}
# ]

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/kafka_mediator/css/kafka_mediator.css"
# app_include_js = "/assets/kafka_mediator/js/kafka_mediator.js"

# include js, css files in header of web template
# web_include_css = "/assets/kafka_mediator/css/kafka_mediator.css"
# web_include_js = "/assets/kafka_mediator/js/kafka_mediator.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "kafka_mediator/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
# doctype_js = {"doctype" : "public/js/doctype.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Svg Icons
# ------------------
# include app icons in desk
# app_include_icons = "kafka_mediator/public/icons.svg"

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
# 	"Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Jinja
# ----------

# add methods and filters to jinja environment
# jinja = {
# 	"methods": "kafka_mediator.utils.jinja_methods",
# 	"filters": "kafka_mediator.utils.jinja_filters"
# }

# Installation
# ------------

# before_install = "kafka_mediator.install.before_install"
# after_install = "kafka_mediator.install.after_install"

# Uninstallation
# ------------

# before_uninstall = "kafka_mediator.uninstall.before_uninstall"
# after_uninstall = "kafka_mediator.uninstall.after_uninstall"

# Integration Setup
# ------------------
# To set up dependencies/integrations with other apps
# Name of the app being installed is passed as an argument

# before_app_install = "kafka_mediator.utils.before_app_install"
# after_app_install = "kafka_mediator.utils.after_app_install"

# Integration Cleanup
# -------------------
# To clean up dependencies/integrations with other apps
# Name of the app being uninstalled is passed as an argument

# before_app_uninstall = "kafka_mediator.utils.before_app_uninstall"
# after_app_uninstall = "kafka_mediator.utils.after_app_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "kafka_mediator.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# DocType Class
# ---------------
# Override standard doctype classes

# override_doctype_class = {
# 	"ToDo": "custom_app.overrides.CustomToDo"
# }

# Document Events
# ---------------
# Hook on document methods and events

# doc_events = {
# 	"*": {
# 		"on_update": "method",
# 		"on_cancel": "method",
# 		"on_trash": "method"
# 	}
# }

# Scheduled Tasks
# ---------------

# scheduler_events = {
# 	"all": [
# 		"kafka_mediator.tasks.all"
# 	],
# 	"daily": [
# 		"kafka_mediator.tasks.daily"
# 	],
# 	"hourly": [
# 		"kafka_mediator.tasks.hourly"
# 	],
# 	"weekly": [
# 		"kafka_mediator.tasks.weekly"
# 	],
# 	"monthly": [
# 		"kafka_mediator.tasks.monthly"
# 	],
# }

scheduler_events = {
    "cron": {
        # Topic stats first — cluster stats aggregate from topic stats written in this pass
        "*/5 * * * *": [
            "kafka_mediator.monitor_kafka_metrics",
            "kafka_mediator.update_topic_stats",
        ],
        # Cluster stats run one minute later so topic stats are always fresh
        "1-56/5 * * * *": [
            "kafka_mediator.update_cluster_stats",
        ],
        # Dead consumer watchdog — cheap DB-only stale-heartbeat check, every minute
        "* * * * *": [
            "kafka_mediator.restart_dead_consumers",
        ],
        # Kafka-native lag+membership heal — broker round-trips, every 5 minutes
        "*/5 * * * *": [
            "kafka_mediator.heal_lagging_groups",
        ],
        # Clean up excess disabled consumer docs every 30 min
        "*/30 * * * *": [
            "kafka_mediator.cleanup_orphaned_consumers",
        ],
    }
}

# Testing
# -------

# before_tests = "kafka_mediator.install.before_tests"

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "kafka_mediator.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "kafka_mediator.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]

# Ignore links to specified DocTypes when deleting documents
# -----------------------------------------------------------

# ignore_links_on_delete = ["Communication", "ToDo"]

# Request Events
# ----------------
# before_request = ["kafka_mediator.utils.before_request"]
# after_request = ["kafka_mediator.utils.after_request"]

# Job Events
# ----------
# before_job = ["kafka_mediator.utils.before_job"]
# after_job = ["kafka_mediator.utils.after_job"]

# User Data Protection
# --------------------

# user_data_fields = [
# 	{
# 		"doctype": "{doctype_1}",
# 		"filter_by": "{filter_by}",
# 		"redact_fields": ["{field_1}", "{field_2}"],
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_2}",
# 		"filter_by": "{filter_by}",
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_3}",
# 		"strict": False,
# 	},
# 	{
# 		"doctype": "{doctype_4}"
# 	}
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
# 	"kafka_mediator.auth.validate"
# ]

# Automatically update python controller files with type annotations for this app.
# export_python_type_annotations = True

# default_log_clearing_doctypes = {
# 	"Logging DocType Name": 30  # days to retain logs
# }

