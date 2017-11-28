''' A collection of html templates used by biothings APIs '''

HTML_OUT_TEMPLATE = '''
    <html>
        <head>
            <link href="https://cdn.rawgit.com/josdejong/jsoneditor/v5.11.0/dist/jsoneditor.min.css" rel="stylesheet" type="text/css">
            <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.12.4/jquery.min.js"></script>
            <script src="https://cdn.rawgit.com/josdejong/jsoneditor/v5.11.0/dist/jsoneditor.min.js"></script>
            <script type="text/javascript">
                $(document).ready(function () {{
                    var container = document.getElementById("jsonview");
                    var options = {{
                        mode: 'view'
                    }};
                    var editor = new JSONEditor(container, options);
                    var data = {};
                    editor.set(data);
                }});
            </script>
        </head>
        <body>
            <div id="jsonview"></div>
        </body>
    </html>'''
