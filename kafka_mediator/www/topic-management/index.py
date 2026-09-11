import frappe

def get_context(context):
    context.csrf_token = frappe.sessions.get_csrf_token()
    context.no_cache = 1
    return context
