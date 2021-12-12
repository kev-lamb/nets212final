$(document).ready(function () {
    $.getJSON('/data/userprofile', function (res) {
        $.post('/getfriends', function (json) {
            let transformed = {
                id: res.Item.username.S,
                name: res.Item.username.S,
                children: [],
            };
            for (friend of json.Items) {
                let toAdd = {
                    id: friend.friend.S,
                    name: friend.friend.S,
                    children: [],
                };
                transformed.children.push(toAdd);
            }

            var infovis = document.getElementById('infovis');
            var w = infovis.offsetWidth - 50,
                h = infovis.offsetHeight - 50;

            //init Hypertree
            var ht = new $jit.Hypertree({
                //id of the visualization container
                injectInto: 'infovis',
                //canvas width and height
                width: w,
                height: h,
                //Change node and edge styles such as
                //color, width and dimensions.
                Node: {
                    //overridable: true,
                    transform: false,
                    color: '#f00',
                },

                Edge: {
                    //overridable: true,
                    color: '#088',
                },
                //calculate nodes offset
                offset: 0.2,
                //Change the animation transition type
                transition: $jit.Trans.Back.easeOut,
                //animation duration (in milliseconds)
                duration: 1000,
                //Attach event handlers and add text to the
                //labels. This method is only triggered on label
                //creation

                onCreateLabel: function (domElement, node) {
                    domElement.innerHTML = node.name;
                    domElement.style.cursor = 'pointer';
                    domElement.onclick = function () {
                        $.getJSON(
                            '/friendvisualizationdata/' + node.id,
                            function (json) {
                                let newNodeData = {
                                    id: node.id,
                                    name: node.name,
                                    children: [],
                                };
                                for (friend of json) {
                                    console.log(friend);
                                    let toAdd = {
                                        id: friend.username.S,
                                        name: friend.username.S,
                                        children: [],
                                    };
                                    newNodeData.children.push(toAdd);
                                }
                                ht.op.sum(newNodeData, {
                                    type: 'fade:seq',
                                    fps: 30,
                                    duration: 1000,
                                    hideLabels: false,
                                    onComplete: function () {
                                        console.log('New nodes added!');
                                    },
                                });
                            }
                        );
                    };
                },
                //Change node styles when labels are placed
                //or moved.
                onPlaceLabel: function (domElement, node) {
                    var width = domElement.offsetWidth;
                    var intX = parseInt(domElement.style.left);
                    intX -= width / 2;
                    domElement.style.left = intX + 'px';
                },

                onComplete: function () {},
            });
            //load JSON data.
            ht.loadJSON(transformed);
            //compute positions and plot.
            ht.refresh();
            //end
            ht.controller.onBeforeCompute(ht.graph.getNode(ht.root));
            ht.controller.onAfterCompute();
            ht.controller.onComplete();
        });
    });
});
