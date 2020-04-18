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
                        <td colspan="2"><p><a href="{link}" target="_blank">{link_decode}</a>&nbsp;&nbsp;[<a href="{docs_link}" target="_blank">Learn more about this API endpoint</a>]</p></td>
                    </tr>
                    <tr><td colspan="2"><div id="jsonview" style="overflow:auto; height: 100%"></div></td></tr>
                </tbody>
            </table>
        </body>
    </html>'''

FRONT_PAGE_TEMPLATE = '''
<!doctype html>
<html lang="en">
    <head>
        <!-- Required meta tags -->
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">

        <!-- Bootstrap CSS -->
        <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css" integrity="sha384-Vkoo8x4CGsO3+Hhxv8T/Q5PaXtkKtu6ug5TOeNV6gBiFeWPGFN9MuhOf23Q9Ifjh" crossorigin="anonymous">

        <title>Hello, world!</title>
    </head>
    <body>
        <div class="container">

            <div class="row mt-5">
                <div class="col">
                <div class="alert alert alert-primary" role="alert">
                    {alert}
                </div>
                </div>
            </div>
            <div class="row">
                <div class="col">
                <div class="jumbotron">
                    <h1 class="display-4"> {title} </h1>
                    <p class="lead"> {text} </p>
                    <hr class="my-4">
                    <p> {footnote} </p>
                    <a class="btn btn-primary btn-lg" href="{url}" role="button">Learn more</a>
                </div>
                </div>
            </div>
        </div>

        <!-- Optional JavaScript -->
        <!-- jQuery first, then Popper.js, then Bootstrap JS -->
        <script src="https://code.jquery.com/jquery-3.4.1.slim.min.js" integrity="sha384-J6qa4849blE2+poT4WnyKhv5vZF5SrPo0iEjwBvKU7imGFAV0wwj1yYfoRSJoZ+n" crossorigin="anonymous"></script>
        <script src="https://cdn.jsdelivr.net/npm/popper.js@1.16.0/dist/umd/popper.min.js" integrity="sha384-Q6E9RHvbIyZFJoft+2mJbHaEWldlvI9IOYy5n3zV9zzTtmI3UksdQRVvoxMfooAo" crossorigin="anonymous"></script>
        <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/js/bootstrap.min.js" integrity="sha384-wfSDF2E50Y2D1uUdj0O3uMBJnjuUD4Ih7YwaYd1iqfktj0Uod8GCExl3Og8ifwB6" crossorigin="anonymous"></script>
    </body>
</html>'''
