from django.contrib import admin

# Register your models here.

from . models import Job, Worker


class JobAdmin(admin.ModelAdmin):
    readonly_fields = ('worker_id')


admin.site.register(Job)
admin.site.register(Worker)