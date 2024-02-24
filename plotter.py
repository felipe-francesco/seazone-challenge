#!/usr/bin/env python3
import matplotlib.pyplot as plt

class Plotter:

    def plot(self, data, y_label, x_label, output_file):
        data = data.head(10)
        fig, ax = plt.subplots(figsize=(10, 6))

        ax.plot(data)

        ax.set(title=f"Correlation {y_label} with {x_label}",
            xlabel=x_label, ylabel=f"Correlation to {y_label}")

        ax.set_xticks(range(len(data.index)))
        ax.set_xticklabels(data.index, rotation=45, ha='right')
        ax.tick_params(axis='x', labelsize=8)
        ax.tick_params(axis='y', labelsize=8)

        ax.grid()
        plt.tight_layout()
        plt.savefig(output_file)

    def plot_bars(self, x_data, y_data, x_label, y_label, title, output_file):
        fig = plt.figure(figsize = (10, 5))
 
        # creating the bar plot
        plt.bar(x_data, y_data, color ='maroon', width = 0.4)
        
        plt.xlabel(x_label, fontsize=10)
        plt.ylabel(y_label, fontsize=10)
        plt.xticks(rotation=45, fontsize=8)
        plt.yticks(fontsize=8)
        plt.title(title)
        plt.tight_layout()
        plt.savefig(output_file)
