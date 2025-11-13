from django.urls import path
from .views import StatementUploadView, StatementListView, process_statement, statement_detail
from . import views

from .views import (
    StatementUploadView,
    StatementListView,
    process_statement,
    statement_detail,
    review_statement,   # <- make sure it is included
)


app_name = "statements"

urlpatterns = [
    path("", StatementListView.as_view(), name="list"),
    path("upload/", StatementUploadView.as_view(), name="upload"),
    path("<int:pk>/process/", process_statement, name="process"),
    path("<int:pk>/detail/", statement_detail, name="detail"),

    # NEW review route
    path("<int:pk>/review/", review_statement, name="review"),

    # ADD THIS BACK
    path("<int:pk>/reprocess/", reprocess_statement, name="reprocess"),
]
