from jinja2 import Template


def render_template(template, context):
    jinja_template = Template(template)
    return jinja_template.render(**context)
