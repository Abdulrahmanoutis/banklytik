from django.urls import path
from .views import StatementUploadView, StatementListView, process_statement, statement_detail
from . import views
from .views import statement_detail
#from .views import statement_detail, chat_with_statement


app_name = "statements"

urlpatterns = [
    path("", views.StatementListView.as_view(), name="list"),
    path("upload/", views.StatementUploadView.as_view(), name="upload"),
    path("<int:pk>/process/", views.process_statement, name="process"),
    path("<int:pk>/detail/", views.statement_detail, name="detail"),
    path("<int:pk>/reprocess/", views.reprocess_statement, name="reprocess"),  # ✅ new
    path('<int:pk>/export-tables/', views.export_tables_for_deepseek, name='export_tables'),
    #path('test-deepseek/', views.test_deepseek_simple, name='test_deepseek'),
    #path("<int:pk>/chat/", views.chat_with_statement, name="chat"),
    #path("<int:pk>/history/", views.chat_history, name="history"),
]

