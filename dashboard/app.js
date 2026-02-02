fetch("data.json")
  .then(r => r.json())
  .then(d => {
    document.getElementById("week").innerText = "Week: " + d.week;
    document.getElementById("totalMsgs").innerText = d.total_messages;
    document.getElementById("totalIssues").innerText = d.total_issues;

    const rate = ((d.total_issues / d.total_messages) * 100).toFixed(1);
    document.getElementById("issueRate").innerText = rate + "%";

    // topics chart
    const ctx = document.getElementById("topicChart");
    new Chart(ctx, {
      type: "bar",
      data: {
        labels: Object.keys(d.topics),
        datasets: [{
          data: Object.values(d.topics)
        }]
      }
    });

    // route table
    const tbody = document.getElementById("routeTable");
    d.routes.forEach(r => {
      tbody.innerHTML += `
        <tr>
          <td>${r.route}</td>
          <td>${r.total}</td>
          <td>${r.issues}</td>
        </tr>`;
    });
  });
