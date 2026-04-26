from uuid import uuid4

from faker import Factory

from users.models import User

faker = Factory.create()


def create_account(username=None, email=None, password=None, name=None, **kwargs):
    """Allow tests to create accounts with explicit or generated unique identities."""
    if username is None:
        username = f"test_{uuid4().hex[:24]}"

    if email is None:
        email = f"{username}@example.com"

    password = password or faker.password()
    name = name or faker.name()

    description = kwargs.get("description") or faker.text()
    is_superuser = kwargs.get("is_superuser") or False
    is_manager = kwargs.get("is_manager") or False
    is_editor = kwargs.get("is_editor") or False

    user = User.objects.create(
        username=username,
        email=email,
        name=name,
        description=description,
        is_superuser=is_superuser,
        is_staff=is_superuser,
        is_editor=is_editor,
        is_manager=is_manager,
    )

    user.set_password(password)
    user.save()
    return user