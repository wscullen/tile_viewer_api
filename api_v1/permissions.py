# api_v1/permissions.py

from rest_framework import permissions

from django.contrib.auth.models import User, Group

class IsOwnerOrReadOnly(permissions.BasePermission):
    """
    Custom permission to only allow owners of an object to edit it.
    """

    def has_object_permission(self, request, view, obj):
        # Read permissions are allowed to any request,
        # so we'll always allow GET, HEAD, or OPTIONS
        if request.method in permissions.SAFE_METHODS:
            return True

        # Write permission are only allowed to the owner of the job
        # OR the super user
        request_user_is_superuser = request.user.groups.filter(name='access_all').exists()
        print("CHECKING PERMISSIONS")
        if obj.owner == request.user or request_user_is_superuser:
            print(obj.owner)
            print(request.user)
            return True
        else:
            print(obj.owner)
            print(request.user)
            return False
        # elif request.user ==
        # do super user check here