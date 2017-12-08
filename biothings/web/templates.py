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
                    var data = {data};
                    editor.set(data);
                }});
            </script>
        </head>
        <body>
            <table style="width:100%; height:100%">
                <tbody>
                    <tr style="height:35px;">
                        <td style="width: 35px"><img src="{img_src}" alt="logo" height="35" width="35"></img></td>
                        <td style="vertical-align:center; padding-left:5px;">{title_html}</td>
                    </tr>
                    <tr style="height:15px;">
                        <td colspan="2"><a href="{link}" target="_blank">{link}</a></td>
                    </tr>
                    <tr><td colspan="2"><div id="jsonview" style="overflow:auto; height: 100%"></div></td></tr>
                </tbody>
            </table>
        </body>
    </html>'''
