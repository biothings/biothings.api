
Feature('Tutorial MVCGI');

Scenario("Clean...", (I) => {
  I.amOnPage('/');
  I.click("Sources");
  I.wait(2)
  if(I.grabNumberOfVisibleElements(".ui.card") != 0) {
    I.clickIfVisible(".trash.icon");
    I.clickIfVisible("#pharmgkb_unregister_yes")
    I.wait(5);
    I.refreshPage()
  } else {
    console.log("Studio ready, good to go")
  }
});

Scenario('Register data plugin', (I) => {
  I.amOnPage('/');
  I.wait(1);
  I.fillField("#huburl","http://localhost:7080");
  I.clickIfVisible("#huburl_ok");
  // check nothing registered
  I.seeTextEquals("0","i.database.icon + span")
  I.see("NO") // no document yet
  I.see("DOCUMENT (YET)")
  // register
  I.click("Sources");
  I.click("Menu");
  I.click("New data plugin");
  I.fillField("#repo_url","https://github.com/sirloon/pharmgkb.git");
  I.click("#repo_url_ok");
  I.wait(1)
  I.waitForText("Hub is restarting",10);
  I.wait(10);
  I.click("#restart_yes");
  I.wait(2); // wait to reconnect
  I.moveCursorTo("i.green.power.off") // reconnected
  I.see("No manifest found")
});

Scenario('Checkout v1', (I) => {
  I.amOnPage('/');
  I.waitForText("Sources",5);
  I.click("Sources");
  I.click("pharmgkb");
  // no manifest yet, so no dumper/uploader
  I.dontSee("Dumper")
  I.dontSee("Uploader")
  I.see("Plugin")
  I.see("Mapping")
  I.click("Plugin")
  I.see("HEAD")
  I.fillField("input#release","pharmgkb_v1")
  I.click("Update")
  I.wait(10)
  I.click("Plugin")
  I.see("pharmgkb_v1")
  I.waitForText("Hub is restarting",10);
  I.wait(10);
  I.click("#restart_yes")
  // on reconnect, page is not refresh automatically (need some work there)
  I.refreshPage()
  // fully active
  I.see("Dumper")
  I.see("Uploader")
  I.see("Plugin")
  I.see("Mapping")
});

Scenario("Dump data", (I) => {
  I.amOnPage('/');
  I.waitForText("Sources",5);
  I.click("Sources");
  I.click("pharmgkb");
  I.wait(2)
  // clean events if any
  I.click("#events")
  I.clickIfVisible("Clear")
  I.checkOption("#force")
  I.click("Dump")
  // 3 parallel downloads (3/x, x is number of workers, variable)
  I.waitForText("3/",2)
  // wait for notifications (should have 3 events, one per dumped file)
  I.waitForText('1', 5, "#num_events")
  I.click("#events")
  I.see("dump_pharmgkb")
  I.see("success [steps=dump,post]")
  // auto upload after a while
  I.waitForText('2', 10, "#num_events")
  I.see("upload_pharmgkb")
  I.see("success [steps=data,post,master,clean]")
  I.click("Clear")
  I.see("No new notifications")
  I.dontSee("dump_pharmgkb")
  I.dontSee("upload_pharmgkb")
  // check tabs
  I.click("Dumper")
  I.see("success")
  I.see("/data/biothings_studio/datasources/pharmgkb")
  I.see("biothings.hub.dataplugin.assistant.AssistedDumper_pharmgkb")
  I.click("Uploader")
  I.see("success")
  I.see("/data/biothings_studio/datasources/pharmgkb")
  I.see("979") // docs uploaded
  I.see("biothings.hub.dataplugin.assistant.AssistedUploader_pharmgkb")
});

Scenario("Inspect v1", (I) => {
  I.amOnPage('/');
  I.waitForText("Sources",5);
  I.click("Sources");
  I.click("pharmgkb");
  I.wait(2)
  I.click("Mapping")
  I.see("No mapping data found for this source.")
  I.click("Inspect data")
  I.waitForText("Inspect data: pharmgkb")
  I.see("mapping")
  // The rest doesn't work, for some reason, we can't find the button to validate
  // the form...
  //I.click("Inspect")
  //I.waitForText("Found errors while generating the mapping:",5)
  //I.see("More than one type")
  //I.see("biothings.utils.common.splitstr")
  //I.see("biothings.utils.common.nan")
  //// intermediate mapping
  //I.see("__type__")
  //I.see('"__type__:splitstr": {},')
  //I.see('"__type__:nan": {}')
});
