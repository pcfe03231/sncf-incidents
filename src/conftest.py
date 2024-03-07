# def pytest_collection_modifyitems(items):
#     """Modifies test items in place to ensure test functions run in a given order"""
#     function_order = [
#         "test_files_icv",
#         "test_import_icv",
#         "test_columns_icv",
#         "test_types_icv"
#         "test_files",
#         "test_import",
#         "test_columns",
#         "test_definitions",
#         "test_types"
#     ]

#     function_mapping = {
#         item: item.name.split("[")[0] if "]" not in function_order[0] else item.name
#         for item in items
#     }

#     sorted_items = items.copy()
#     for func_ in function_order:
#         sorted_items = [it for it in sorted_items if function_mapping[it] != func_] + [
#             it for it in sorted_items if function_mapping[it] == func_
#         ]
#     items[:] = sorted_items
