from biothings.hub.databuild.buildconfig import AutoBuildConfig


def test_autobuildconfig_construction():
    mygene = AutoBuildConfig({"autobuild": {"schedule": "0 8 * * 7"}})
    print(mygene.export())
    assert mygene.should_diff_new_build()
    assert not mygene.should_snapshot_new_build()
    assert not mygene.should_publish_new_diff()
    assert not mygene.should_publish_new_snapshot()
    assert not mygene.should_install_new_diff()
    assert not mygene.should_install_new_snapshot()
    assert not mygene.should_install_new_release()

    outbreak = AutoBuildConfig(
        {
            "autobuild": {
                "schedule": "0 8 * * *",
                "type": "snapshot",
                "env": "auto",  # snapshot location, indexing ES.
            },
            "autopublish": {"type": "snapshot", "env": "auto"},
            "autorelease": {"schedule": "0 10 * * *"},
        }
    )
    print(outbreak.export())
    assert not outbreak.should_diff_new_build()
    assert outbreak.should_snapshot_new_build()
    assert not outbreak.should_publish_new_diff()
    assert outbreak.should_publish_new_snapshot()
    assert not outbreak.should_install_new_diff()
    assert not outbreak.should_install_new_snapshot()
    assert outbreak.should_install_new_release()
